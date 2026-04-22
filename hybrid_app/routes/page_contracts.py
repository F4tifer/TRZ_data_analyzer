import json


def normalize_query_values(values: list[str]) -> list[str]:
    return [str(v).strip() for v in values if str(v).strip()]


def hx_trigger_header(
    *,
    events: list[str] | None = None,
    toast_level: str | None = None,
    toast_message: str | None = None,
) -> dict[str, str]:
    payload: dict[str, object] = {}
    for event in events or []:
        payload[str(event)] = True
    if toast_level:
        payload[f"toast-{toast_level}"] = {"message": toast_message or ""}
    if not payload:
        return {}
    return {"HX-Trigger": json.dumps(payload)}
