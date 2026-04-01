from __future__ import annotations

from typing import Any, Iterable


OK_VALUES = {"", "ok", "correto", "sem divergencia", "sem divergência"}
DIVERGENT_VALUES = {"divergente", "ausente", "erro", "inconsistente"}

FINAL_STATUS_FIELDS = (
    "status_csrf",
    "status_irrf",
    "status_inss",
    "status_base_calculo",
    "status_valor_liquido",
)


def normalize_status_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def has_text_flag(value: Any) -> bool:
    return bool(str(value or "").strip())


def normalize_manual_queue_status(value: Any) -> str | None:
    normalized = normalize_status_value(value)
    if not normalized:
        return None
    if normalized in OK_VALUES:
        return "correta"
    if normalized in DIVERGENT_VALUES:
        return "divergente"
    return str(value).strip() or None


def is_divergent_status_value(value: Any) -> bool:
    normalized = normalize_status_value(value)
    if not normalized:
        return False
    if normalized in DIVERGENT_VALUES:
        return True
    return normalized not in OK_VALUES


def compute_final_note_status(payload: dict[str, Any], fields: Iterable[str] = FINAL_STATUS_FIELDS) -> str:
    for field in fields:
        if is_divergent_status_value(payload.get(field)):
            return "divergente"
    return "correta"


def compute_queue_state(payload: dict[str, Any]) -> dict[str, Any]:
    manual_status = normalize_manual_queue_status(payload.get("status_fila_manual"))
    automatic_status = compute_final_note_status(payload)
    simples_status = payload.get("status_simples_nacional")
    possui_campos_ausentes = has_text_flag(payload.get("campos_ausentes_xml"))
    possui_alertas = has_text_flag(payload.get("alertas_fiscais"))
    simples_divergente = is_divergent_status_value(simples_status)

    if manual_status == "correta":
        status_fila_final = "correta"
        divergencia_fila_final = False
    elif manual_status == "divergente":
        status_fila_final = "divergente"
        divergencia_fila_final = True
    else:
        divergencia_fila_final = any(
            (
                automatic_status == "divergente",
                simples_divergente,
                possui_campos_ausentes,
            )
        )
        status_fila_final = "divergente" if divergencia_fila_final else "correta"

    return {
        "status_automatico_fiscal": automatic_status,
        "status_fila_manual_normalizado": manual_status,
        "status_fila_final": status_fila_final,
        "divergencia_fila_final": divergencia_fila_final,
        "divergencia_fila_label": "Com divergência" if divergencia_fila_final else "Sem divergência",
        "possui_campos_ausentes_xml": possui_campos_ausentes,
        "possui_alertas_fiscais": possui_alertas,
        "divergencia_simples_nacional": simples_divergente,
    }


def compute_base_calculation_status(
    valor_bc: float | None,
    valor_total: float | None,
    tolerance: float = 0.01,
) -> str:
    if valor_bc is None:
        return "ausente"
    if valor_bc < -tolerance:
        return "divergente"
    if valor_total is not None and valor_bc - valor_total > tolerance:
        return "divergente"
    return "ok"


def build_sql_status_expr(alias: str = "n") -> str:
    ok_values_sql = ", ".join(f"'{value}'" for value in sorted(OK_VALUES))
    conditions = [
        f"LOWER(COALESCE({alias}.{field}, 'ok')) IN ({ok_values_sql})"
        for field in FINAL_STATUS_FIELDS
    ]
    return """(
    CASE
      WHEN {conditions}
      THEN 'correta'
      ELSE 'divergente'
    END
)""".format(conditions="\n       AND ".join(conditions))


def build_sql_queue_status_expr(alias: str = "n", status_expr: str | None = None) -> str:
    automatic_status_expr = status_expr or build_sql_status_expr(alias)
    manual_status_expr = f"LOWER(BTRIM(COALESCE({alias}.status_fila_manual, '')))"
    simples_status_expr = f"LOWER(BTRIM(COALESCE({alias}.status_simples_nacional, '')))"
    has_campos_ausentes_expr = f"NULLIF(BTRIM(COALESCE({alias}.campos_ausentes_xml, '')), '') IS NOT NULL"
    ok_values_sql = ", ".join(f"'{value}'" for value in sorted(OK_VALUES))
    divergent_values_sql = ", ".join(f"'{value}'" for value in sorted(DIVERGENT_VALUES))

    return f"""(
    CASE
      WHEN {manual_status_expr} IN ({ok_values_sql}) THEN 'correta'
      WHEN {manual_status_expr} IN ({divergent_values_sql}) THEN 'divergente'
      WHEN {automatic_status_expr} = 'divergente'
        OR ({simples_status_expr} <> '' AND {simples_status_expr} NOT IN ({ok_values_sql}))
        OR {has_campos_ausentes_expr}
      THEN 'divergente'
      ELSE 'correta'
    END
)"""


def build_sql_queue_divergence_expr(alias: str = "n", status_expr: str | None = None) -> str:
    queue_status_expr = build_sql_queue_status_expr(alias, status_expr=status_expr)
    return f"(({queue_status_expr}) = 'divergente')"
