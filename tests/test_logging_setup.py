from sniper_bot.logging_setup import _redact_event


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
