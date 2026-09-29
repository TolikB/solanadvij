"""Local stand-ins for Jupiter and the Helius RPC, for network-free rehearsals.

One threaded HTTP server answers Jupiter's quote-only ``GET /order`` and the
read-only JSON-RPC methods the security checks use, from a price the test
moves. The bot talks to it through its real clients, configured with
``providers.jupiter_base_url`` and ``HELIUS_RPC_URL``.
"""

from __future__ import annotations

import json
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"


class FakeProviders:
    def __init__(self, *, quote_mint: str, token_mint: str) -> None:
        self.quote_mint = quote_mint
        self.token_mint = token_mint
        # USD per raw token unit; the quote asset has 6 decimals.
        self.price_usd = Decimal("0.000001")
        self.supply_raw = 1_000_000_000_000
        self.holders = [
            ("holder-a", 400_000_000_000),
            ("holder-b", 350_000_000_000),
            ("holder-c", 250_000_000_000),
        ]
        self.requests: list[str] = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def __enter__(self) -> "FakeProviders":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()

    def _order(self, query: dict[str, list[str]]) -> dict[str, Any]:
        input_mint = query["inputMint"][0]
        amount = Decimal(query["amount"][0])
        if input_mint == self.quote_mint:
            usd = amount / Decimal(10**6)
            out = (usd / self.price_usd).to_integral_value()
            return {
                "inAmount": str(amount),
                "outAmount": str(out),
                "inUsdValue": str(usd),
                "outUsdValue": str(usd),
                "priceImpactPct": "0.001",
                "routePlan": [{"label": "fake"}],
            }
        usd = amount * self.price_usd
        return {
            "inAmount": str(amount),
            "outAmount": str((usd * Decimal(10**6)).to_integral_value()),
            "inUsdValue": str(usd),
            "outUsdValue": str(usd),
            "priceImpactPct": "0.001",
            "routePlan": [{"label": "fake"}],
        }

    def _rpc(self, method: str) -> Any:
        if method == "getAccountInfo":
            return {
                "value": {
                    "owner": TOKEN_PROGRAM,
                    "data": {
                        "parsed": {
                            "info": {
                                "decimals": 6,
                                "supply": str(self.supply_raw),
                                "mintAuthority": None,
                                "freezeAuthority": None,
                            }
                        }
                    },
                }
            }
        if method == "getTokenAccounts":
            return {
                "token_accounts": [
                    {"address": f"account-{owner}", "owner": owner, "amount": amount}
                    for owner, amount in self.holders
                ],
                "last_indexed_slot": 100,
                "cursor": None,
            }
        if method == "getSlot":
            return 105
        raise ValueError(f"unsupported fake RPC method {method}")

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        providers = self

        class Handler(BaseHTTPRequestHandler):
            def _reply(self, payload: Any) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                providers.requests.append(f"GET {parsed.path}")
                self._reply(providers._order(parse_qs(parsed.query)))

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                request = json.loads(self.rfile.read(length))
                providers.requests.append(f"RPC {request['method']}")
                self._reply(
                    {
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": providers._rpc(request["method"]),
                    }
                )

            def log_message(self, *args: object) -> None:
                return

        return Handler
