import logging

from sniper_bot.logging_setup import _redact_event, configure_logging


def test_structured_logging_redacts_nested_secrets() -> None:
    event = _redact_event(
        None,
        "info",
        {
            "event": "request",
            "api_key": "secret-value",
            "payload": {"telegram_token": "token-value", "mint": "safe"},
        },
    )

    assert event == {
        "event": "request",
        "api_key": "***",
        "payload": {"telegram_token": "***", "mint": "safe"},
    }


def test_structured_logging_redacts_secrets_embedded_in_urls() -> None:
    event = _redact_event(
        None,
        "warning",
        {
            "event": (
                "request failed https://rpc.example/?api-key=helius-secret "
                "https://api.telegram.org/bot123456789:telegram-secret/getUpdates"
            )
        },
    )

    rendered = str(event["event"])
    assert "helius-secret" not in rendered
    assert "telegram-secret" not in rendered
    assert "api-key=***" in rendered
    assert "/bot***" in rendered


def test_configure_logging_suppresses_verbose_http_client_access_logs() -> None:
    configure_logging()

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING
