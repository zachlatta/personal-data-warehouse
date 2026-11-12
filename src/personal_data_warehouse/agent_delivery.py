"""Shared SQL for identifying agent delivery calls and their received prompts."""

DELIVERY_WINDOW_MINUTES = 10


def delivery_call_predicate(alias: str) -> str:
    """Keep the adapter predicate identical to the partial source indexes."""
    prefix = alias + "." if alias else ""
    return (
        f"({prefix}tool_input_json<>'' AND ("
        f"{prefix}tool_name ~* '(send_agent_prompt|create_agent|spawn_agent)$' "
        f"OR ({prefix}tool_name ~* '(bash|exec|shell|run_command|command)' "
        f"AND {prefix}tool_input_json ~* 'paseo\\M.{{0,160}}\\m(send|run)\\M')))"
    )


def delivery_text(expression: str) -> str:
    """Compare text across JSON/shell escaping without executing shell expansion."""
    return (
        "btrim(regexp_replace("
        f"replace(replace(replace(replace({expression}, E'\\\\n',' '), "
        "E'\\\\r',' '), E'\\\\t',' '), E'\\\\',''), "
        "'[[:space:]]+',' ','g'))"
    )


def delivery_text_matches(sent: str, received: str) -> str:
    """Require a full short payload or three disjoint long-payload fingerprints.

    Shell scripts can interpolate a path, timestamp or port. Three of four
    32-character fingerprints tolerate one changed segment while retaining
    evidence from separate parts of the delivered prompt. Short common replies
    lack sufficient evidence without an ingested recipient/launch registry.
    """
    positions = (
        "1",
        f"(length({received})-32)/3+1",
        f"2*(length({received})-32)/3+1",
        f"length({received})-31",
    )
    hits = " + ".join(
        f"(strpos({sent},substring({received} FROM {position} FOR 32))>0)::int"
        for position in positions
    )
    return (
        f"(length({received})>=48 AND CASE WHEN length({received})<128 "
        f"THEN strpos({sent},{received})>0 ELSE ({hits})>=3 END)"
    )
