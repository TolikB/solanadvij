"""Strict decoder for Anchor events described by a vendored IDL."""

from __future__ import annotations

import base64
import json
import struct
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class AnchorDecodeError(ValueError):
    pass


class UnknownDiscriminatorError(AnchorDecodeError):
    def __init__(self, program_id: str, discriminator: bytes) -> None:
        self.program_id = program_id
        self.discriminator = discriminator
        super().__init__(
            f"unknown Anchor event discriminator for {program_id}: {discriminator.hex()}"
        )


@dataclass(frozen=True)
class AnchorEvent:
    name: str
    fields: dict[str, Any]
    log_index: int


@dataclass(frozen=True)
class AnchorLogScan:
    """Selected events of one transaction plus its on-chain clock timestamp.

    ``timestamp`` is the ``Clock::unix_timestamp`` the program wrote into the
    first own event that carries one. It is the value ``getBlockTime`` returns
    for the slot, so it dates a transaction without an RPC round trip, and it is
    read even from events that are skipped rather than decoded.
    """

    events: list[AnchorEvent]
    timestamp: int | None
    # Discriminators the vendored IDL does not know, in first-seen order.
    unknown_discriminators: tuple[bytes, ...] = ()
    # Selected events that carried bytes beyond every field the IDL declares.
    appended_events: tuple[str, ...] = ()


class _Cursor:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    def take(self, size: int) -> bytes:
        if size < 0 or self.offset + size > len(self.payload):
            raise AnchorDecodeError("truncated Anchor event payload")
        result = self.payload[self.offset : self.offset + size]
        self.offset += size
        return result

    @property
    def remaining(self) -> int:
        return len(self.payload) - self.offset


class AnchorIdlDecoder:
    def __init__(self, idl_path: str | Path) -> None:
        self.idl_path = Path(idl_path)
        with self.idl_path.open("r", encoding="utf-8") as stream:
            self.idl = json.load(stream)
        self.program_id = str(self.idl["address"])
        self._types = {item["name"]: item["type"] for item in self.idl.get("types", [])}
        self._events: dict[bytes, str] = {
            bytes(item["discriminator"]): item["name"] for item in self.idl.get("events", [])
        }
        self._timestamp_offsets: dict[str, int] = {}
        for event_name in self._events.values():
            offset = self._fixed_field_offset(event_name, "timestamp")
            if offset is not None:
                self._timestamp_offsets[event_name] = offset

    def decode_logs(self, logs: list[str]) -> list[AnchorEvent]:
        """Decode every own event strictly; unknown discriminators fail closed."""
        return self.scan_logs(logs).events

    def scan_logs(
        self,
        logs: list[str],
        *,
        event_names: frozenset[str] | None = None,
        minimum_fields: Mapping[str, str] | None = None,
    ) -> AnchorLogScan:
        """Decode the selected own events and read the transaction timestamp.

        With ``event_names`` only those events are decoded and every other
        discriminator, including one the vendored IDL does not know, is
        skipped after reading at most its fixed-offset timestamp.

        ``minimum_fields`` names, per event, the last field of the oldest
        layout still accepted. Such an event must decode at least through that
        field; later fields are decoded while bytes remain and the payload ends
        on a field boundary, and bytes beyond every declared field are reported
        in ``appended_events`` instead of failing, because programs extend
        events by appending fields. Anything else that does not fit the IDL
        still fails closed.
        """
        if "Log truncated" in logs:
            raise AnchorDecodeError("truncated logs require verified CPI completeness")
        decoded: list[AnchorEvent] = []
        timestamp: int | None = None
        unknown: dict[bytes, None] = {}
        appended: list[str] = []
        for log_index, encoded in self._own_event_lines(logs):
            if event_names is None:
                payload = _b64decode(encoded)
                prefix = payload
            else:
                prefix = _b64decode_prefix(encoded, 8)
            if len(prefix) < 8:
                raise AnchorDecodeError("Anchor event payload is shorter than discriminator")
            discriminator = prefix[:8]
            event_name = self._events.get(discriminator)
            if event_name is None:
                if event_names is not None:
                    unknown.setdefault(discriminator, None)
                    continue
                raise UnknownDiscriminatorError(self.program_id, discriminator)
            if event_names is not None and event_name not in event_names:
                if timestamp is None:
                    timestamp = self._peek_timestamp(event_name, encoded)
                continue
            payload = prefix if event_names is None else _b64decode(encoded)
            minimum_field = (minimum_fields or {}).get(event_name)
            if minimum_field is None:
                fields = self._decode_struct(event_name, payload[8:])
            else:
                fields, trailing = self._decode_struct_from_minimum(
                    event_name, payload[8:], minimum_field
                )
                if trailing:
                    appended.append(event_name)
            if timestamp is None and type(fields.get("timestamp")) is int:
                timestamp = int(fields["timestamp"])
            decoded.append(
                AnchorEvent(name=event_name, fields=fields, log_index=log_index)
            )
        return AnchorLogScan(
            events=decoded,
            timestamp=timestamp,
            unknown_discriminators=tuple(unknown),
            appended_events=tuple(appended),
        )

    def verified_cpi_log_prefix(
        self,
        transaction: Mapping[str, Any],
        logs: list[str],
        *,
        event_names: frozenset[str],
        instruction_events: Mapping[str, str],
        minimum_fields: Mapping[str, str] | None = None,
        event_validator: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> list[str]:
        """Keep original log indices only when the ledger proves completeness.

        Small logs after the runtime truncation marker have lost invocation
        boundaries. For successful jsonParsed metadata, require exactly the
        expected event CPI inside every allowlisted own operation. All consumed
        CPI payloads must also match the original prefix in execution order.
        Missing events fail closed; no event key is synthesized.
        """
        prefix = logs[:logs.index("Log truncated")]
        inner = transaction.get("transaction")
        if not isinstance(inner, dict):
            raise AnchorDecodeError("truncated logs require full transaction metadata")
        message = inner.get("message")
        meta = transaction.get("meta") or inner.get("meta")
        if (
            not isinstance(message, dict)
            or not isinstance(meta, dict)
            or "err" not in meta
            or meta["err"] is not None
        ):
            raise AnchorDecodeError("truncated logs require successful transaction metadata")
        outer = message.get("instructions")
        groups = meta.get("innerInstructions")
        if not isinstance(outer, list) or not isinstance(groups, list):
            raise AnchorDecodeError("truncated logs require complete inner instructions")
        grouped: dict[int, list[Any]] = {}
        previous_index = -1
        for group in groups:
            if not isinstance(group, dict):
                raise AnchorDecodeError("invalid truncated transaction instruction group")
            index = group.get("index")
            values = group.get("instructions")
            if (
                type(index) is not int
                or not previous_index < index < len(outer)
                or not isinstance(values, list)
            ):
                raise AnchorDecodeError("invalid truncated transaction instruction group")
            previous_index = index
            grouped[index] = values
        names = {
            bytes(item["discriminator"]): item["name"]
            for item in self.idl.get("instructions", [])
        }
        expected: list[str] = []
        emitted: list[int] = []
        selected: list[bytes] = []
        for index, root in enumerate(outer):
            stack: list[tuple[int, str, int | None]] = []
            for position, instruction in enumerate([root, *grouped.get(index, [])]):
                if not isinstance(instruction, dict) or not isinstance(instruction.get("programId"), str):
                    raise AnchorDecodeError("truncated logs require jsonParsed instruction program IDs")
                height = 1 if position == 0 else instruction.get("stackHeight")
                if type(height) is not int or height < 1 or (position and height < 2):
                    raise AnchorDecodeError("truncated logs require valid instruction stack heights")
                while stack and stack[-1][0] >= height:
                    stack.pop()
                if height > 1 and (not stack or stack[-1][0] != height - 1):
                    raise AnchorDecodeError("truncated transaction instruction stack is incomplete")
                program_id = instruction["programId"]
                operation: int | None = None
                if program_id == self.program_id:
                    encoded = instruction.get("data")
                    if not isinstance(encoded, str) or not encoded or len(encoded) > 16384:
                        raise AnchorDecodeError("invalid own instruction in truncated transaction")
                    payload = _base58_decode(encoded)
                    if payload.startswith(_EVENT_IX_TAG):
                        if not stack or stack[-1][1] != self.program_id or stack[-1][2] is None:
                            raise AnchorDecodeError("Anchor CPI event has no own parent operation")
                        if len(payload) < 16:
                            raise AnchorDecodeError("truncated Anchor CPI event payload")
                        parent = stack[-1][2]
                        event_name = self._events.get(payload[8:16])
                        if event_name != expected[parent] or emitted[parent]:
                            raise AnchorDecodeError("CPI event does not cover its parent operation")
                        emitted[parent] += 1
                        if event_name not in event_names or event_validator is not None:
                            # Ignored CPI events still require a valid body.
                            minimum = (minimum_fields or {}).get(event_name)
                            if minimum is None:
                                fields = self._decode_struct(event_name, payload[16:])
                            else:
                                fields, _ = self._decode_struct_from_minimum(event_name, payload[16:], minimum)
                            if event_validator is not None:
                                event_validator(event_name, fields)
                        if event_name in event_names:
                            selected.append(payload[8:])
                    else:
                        name = names.get(payload[:8])
                        if not isinstance(name, str) or name not in instruction_events:
                            raise AnchorDecodeError("unverified own operation in truncated transaction")
                        operation = len(expected)
                        expected.append(instruction_events[name])
                        emitted.append(0)
                stack.append((height, program_id, operation))
        if not expected or any(count != 1 for count in emitted):
            raise AnchorDecodeError("CPI events do not cover truncated transaction operations")
        logged: list[bytes] = []
        for _, encoded in self._own_event_lines(prefix):
            payload = _b64decode(encoded)
            if len(payload) < 8:
                raise AnchorDecodeError("Anchor event payload is shorter than discriminator")
            if self._events.get(payload[:8]) in event_names:
                logged.append(payload)
        if logged != selected:
            raise AnchorDecodeError("consumed events are missing from the verified log prefix")
        return prefix

    def declared_fields(self, event_name: str) -> list[str]:
        definition = self._types.get(event_name)
        if definition is None or definition.get("kind") != "struct":
            raise AnchorDecodeError(f"missing struct definition for Anchor event {event_name}")
        return [str(item["name"]) for item in definition.get("fields", [])]

    def _decode_struct_from_minimum(
        self, type_name: str, payload: bytes, minimum_field: str
    ) -> tuple[dict[str, Any], int]:
        definition = self._types.get(type_name)
        if definition is None or definition.get("kind") != "struct":
            raise AnchorDecodeError(f"missing struct definition for Anchor event {type_name}")
        cursor = _Cursor(payload)
        fields: dict[str, Any] = {}
        required = True
        for item in definition.get("fields", []):
            if not required and cursor.remaining == 0:
                # An older deployed layout ends here.
                break
            fields[item["name"]] = self._decode_type(item["type"], cursor)
            if item["name"] == minimum_field:
                required = False
        if required:
            raise AnchorDecodeError(
                f"Anchor event {type_name} does not declare minimum field {minimum_field}"
            )
        return fields, cursor.remaining

    def _own_event_lines(self, logs: list[str]) -> list[tuple[int, str]]:
        lines: list[tuple[int, str]] = []
        program_stack: list[str] = []
        own_invocation_seen = False

        for log_index, line in enumerate(logs):
            if line.startswith("Program ") and " invoke [" in line:
                program_id = line.split(" ", 2)[1]
                program_stack.append(program_id)
                own_invocation_seen = own_invocation_seen or program_id == self.program_id
                continue
            if line.startswith("Program ") and (
                line.endswith(" success") or " failed:" in line
            ):
                if program_stack:
                    program_stack.pop()
                continue
            if not line.startswith("Program data: "):
                continue
            if program_stack and program_stack[-1] != self.program_id:
                continue
            if not program_stack and not own_invocation_seen:
                continue
            lines.append((log_index, line.removeprefix("Program data: ").strip()))
        return lines

    def _peek_timestamp(self, event_name: str, encoded: str) -> int | None:
        offset = self._timestamp_offsets.get(event_name)
        if offset is None:
            return None
        start = 8 + offset
        payload = _b64decode_prefix(encoded, start + 8)
        if len(payload) < start + 8:
            return None
        return int(struct.unpack("<q", payload[start : start + 8])[0])

    def _fixed_field_offset(self, type_name: str, field_name: str) -> int | None:
        definition = self._types.get(type_name)
        if definition is None or definition.get("kind") != "struct":
            return None
        offset = 0
        for field in definition.get("fields", []):
            if field["name"] == field_name:
                return offset if field["type"] == "i64" else None
            size = self._fixed_size(field["type"])
            if size is None:
                return None
            offset += size
        return None

    def _fixed_size(self, type_spec: Any) -> int | None:
        if isinstance(type_spec, str):
            return _FIXED_PRIMITIVE_SIZES.get(type_spec)
        if not isinstance(type_spec, dict):
            return None
        if "array" in type_spec:
            item_type, length = type_spec["array"]
            item_size = self._fixed_size(item_type)
            return None if item_size is None else item_size * int(length)
        if "defined" in type_spec:
            defined = type_spec["defined"]
            name = defined["name"] if isinstance(defined, dict) else str(defined)
            definition = self._types.get(name)
            if definition is None or definition.get("kind") != "struct":
                return None
            total = 0
            for field in definition.get("fields", []):
                size = self._fixed_size(field["type"])
                if size is None:
                    return None
                total += size
            return total
        return None

    def _decode_struct(self, type_name: str, payload: bytes) -> dict[str, Any]:
        definition = self._types.get(type_name)
        if definition is None or definition.get("kind") != "struct":
            raise AnchorDecodeError(f"missing struct definition for Anchor event {type_name}")
        cursor = _Cursor(payload)
        fields = {
            field["name"]: self._decode_type(field["type"], cursor)
            for field in definition.get("fields", [])
        }
        if cursor.remaining:
            raise AnchorDecodeError(
                f"Anchor event {type_name} has {cursor.remaining} unexpected trailing bytes"
            )
        return fields

    def _decode_type(self, type_spec: Any, cursor: _Cursor) -> Any:
        if isinstance(type_spec, str):
            return self._decode_primitive(type_spec, cursor)
        if not isinstance(type_spec, dict):
            raise AnchorDecodeError(f"unsupported IDL type: {type_spec!r}")
        if "option" in type_spec:
            present = self._decode_primitive("u8", cursor)
            if present == 0:
                return None
            if present != 1:
                raise AnchorDecodeError("invalid Borsh option tag")
            return self._decode_type(type_spec["option"], cursor)
        if "vec" in type_spec:
            length = self._decode_primitive("u32", cursor)
            if length > 100_000:
                raise AnchorDecodeError("unreasonable Borsh vector length")
            return [self._decode_type(type_spec["vec"], cursor) for _ in range(length)]
        if "array" in type_spec:
            item_type, length = type_spec["array"]
            return [self._decode_type(item_type, cursor) for _ in range(int(length))]
        if "defined" in type_spec:
            defined = type_spec["defined"]
            name = defined["name"] if isinstance(defined, dict) else str(defined)
            definition = self._types.get(name)
            if definition is None:
                raise AnchorDecodeError(f"missing IDL type definition {name}")
            if definition.get("kind") == "struct":
                return {
                    field["name"]: self._decode_type(field["type"], cursor)
                    for field in definition.get("fields", [])
                }
            if definition.get("kind") == "enum":
                variant_index = self._decode_primitive("u8", cursor)
                variants = definition.get("variants", [])
                if variant_index >= len(variants):
                    raise AnchorDecodeError(f"invalid enum variant for {name}")
                variant = variants[variant_index]
                return variant["name"]
            raise AnchorDecodeError(f"unsupported defined type kind for {name}")
        raise AnchorDecodeError(f"unsupported IDL type: {type_spec!r}")

    def _decode_primitive(self, name: str, cursor: _Cursor) -> Any:
        formats = {
            "u8": ("<B", 1),
            "i8": ("<b", 1),
            "u16": ("<H", 2),
            "i16": ("<h", 2),
            "u32": ("<I", 4),
            "i32": ("<i", 4),
            "u64": ("<Q", 8),
            "i64": ("<q", 8),
        }
        if name in formats:
            fmt, size = formats[name]
            return struct.unpack(fmt, cursor.take(size))[0]
        if name == "u128":
            return int.from_bytes(cursor.take(16), "little", signed=False)
        if name == "i128":
            return int.from_bytes(cursor.take(16), "little", signed=True)
        if name == "bool":
            value = self._decode_primitive("u8", cursor)
            if value not in (0, 1):
                raise AnchorDecodeError("invalid Borsh bool")
            return bool(value)
        if name == "pubkey":
            return _base58_encode(cursor.take(32))
        if name == "string":
            length = self._decode_primitive("u32", cursor)
            if length > 1_000_000:
                raise AnchorDecodeError("unreasonable Borsh string length")
            try:
                return cursor.take(length).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise AnchorDecodeError("invalid UTF-8 Borsh string") from exc
        if name in {"bytes"}:
            length = self._decode_primitive("u32", cursor)
            return cursor.take(length).hex()
        raise AnchorDecodeError(f"unsupported primitive IDL type {name}")


_FIXED_PRIMITIVE_SIZES = {
    "u8": 1,
    "i8": 1,
    "bool": 1,
    "u16": 2,
    "i16": 2,
    "u32": 4,
    "i32": 4,
    "u64": 8,
    "i64": 8,
    "u128": 16,
    "i128": 16,
    "pubkey": 32,
}


def _b64decode(encoded: str) -> bytes:
    try:
        return base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise AnchorDecodeError("invalid base64 in Anchor event log") from exc


def _b64decode_prefix(encoded: str, byte_count: int) -> bytes:
    """Decode only the leading base64 groups that cover ``byte_count`` bytes."""
    characters = -(-byte_count // 3) * 4
    if len(encoded) <= characters:
        return _b64decode(encoded)
    return _b64decode(encoded[:characters])


_BASE58_ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _base58_encode(payload: bytes) -> str:
    leading_zeroes = len(payload) - len(payload.lstrip(b"\x00"))
    number = int.from_bytes(payload, "big")
    encoded = bytearray()
    while number:
        number, remainder = divmod(number, 58)
        encoded.append(_BASE58_ALPHABET[remainder])
    encoded.extend(_BASE58_ALPHABET[0] for _ in range(leading_zeroes))
    encoded.reverse()
    return encoded.decode("ascii") or "1" * leading_zeroes


# Anchor's EVENT_IX_TAG_LE, checked before treating an instruction as an event.
_EVENT_IX_TAG = bytes.fromhex("e445a52e51cb9a1d")


def _base58_decode(encoded: str) -> bytes:
    number = 0
    for character in encoded.encode("ascii", errors="replace"):
        digit = _BASE58_ALPHABET.find(bytes([character]))
        if digit < 0:
            raise AnchorDecodeError("invalid base58 in Anchor CPI instruction")
        number = number * 58 + digit
    leading_zeroes = len(encoded) - len(encoded.lstrip("1"))
    return bytes(leading_zeroes) + number.to_bytes((number.bit_length() + 7) // 8, "big")
