"""Strict decoder for Anchor events described by a vendored IDL."""

from __future__ import annotations

import base64
import json
import struct
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

    def decode_logs(self, logs: list[str]) -> list[AnchorEvent]:
        decoded: list[AnchorEvent] = []
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
            encoded = line.removeprefix("Program data: ").strip()
            try:
                payload = base64.b64decode(encoded, validate=True)
            except Exception as exc:
                raise AnchorDecodeError("invalid base64 in Anchor event log") from exc
            if len(payload) < 8:
                raise AnchorDecodeError("Anchor event payload is shorter than discriminator")
            discriminator = payload[:8]
            event_name = self._events.get(discriminator)
            if event_name is None:
                raise UnknownDiscriminatorError(self.program_id, discriminator)
            decoded.append(
                AnchorEvent(
                    name=event_name,
                    fields=self._decode_struct(event_name, payload[8:]),
                    log_index=log_index,
                )
            )
        return decoded

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
