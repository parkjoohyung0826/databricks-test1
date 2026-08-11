# Databricks notebook source
"""Small, side-effect-free notebook used to test Lakeflow Jobs deployment."""


def normalize_message(message: str) -> str:
    """Trim surrounding whitespace and reject an empty job parameter."""
    normalized = message.strip()
    if not normalized:
        raise ValueError("message must not be empty")
    return normalized


def build_result(message: str) -> dict[str, str]:
    """Build the deterministic result emitted by the example job."""
    return {
        "status": "ok",
        "message": normalize_message(message),
    }


if __name__ == "__main__":
    dbutils.widgets.text("message", "hello databricks")  # type: ignore[name-defined]
    result = build_result(dbutils.widgets.get("message"))  # type: ignore[name-defined]
    print(result)

