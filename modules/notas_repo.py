from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from psycopg.types.json import Jsonb

from .db import get_conn
from .fiscal_status import build_sql_status_expr, compute_base_calculation_status
from .nfse_keys import gerar_chave_nfse


STATUS_EXPR = build_sql_status_expr("n")


def garantir_schema_nfse_notas():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS nfse_notas (
          id BIGSERIAL PRIMARY KEY,
          cert_alias TEXT NOT NULL,
          processo_id UUID,
          chave_nfse TEXT NOT NULL,
          numero_documento TEXT,
          competencia TEXT,
          data_emissao DATE,
          municipio TEXT,
          cnpj_prestador TEXT,
          razao_social TEXT,
          valor_total NUMERIC,
          valor_bc NUMERIC,
          valor_liquido NUMERIC,
          valor_liquido_correto NUMERIC,
          csrf NUMERIC,
          irrf NUMERIC,
          percentual_irrf NUMERIC,
          inss NUMERIC,
          iss NUMERIC,
          retencao_csrf TEXT,
          incidencia_iss TEXT,
          data_pagamento TEXT,
          codigo_servico TEXT,
          descricao_servico TEXT,
          codigo_nbs TEXT,
          codigo_cnae TEXT,
          descricao_cnae TEXT,
          simples_xml TEXT,
          consulta_simples_api TEXT,
          status_simples_nacional TEXT,
          status_csrf TEXT,
          status_irrf TEXT,
          status_inss TEXT,
          status_base_calculo TEXT,
          status_valor_liquido TEXT,
          campos_ausentes_xml TEXT,
          alertas_fiscais TEXT,
          tipo_nota TEXT,
          parte_exibicao_nome TEXT,
          parte_exibicao_doc TEXT,
          parte_exibicao_tipo TEXT,
          dados_completos JSONB NOT NULL,
          arquivo_origem TEXT,
          created_at TIMESTAMP NOT NULL DEFAULT now(),
          updated_at TIMESTAMP NOT NULL DEFAULT now()
        );
        """)
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS cert_alias TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS processo_id UUID")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS chave_nfse TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS tipo_nota TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS parte_exibicao_nome TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS parte_exibicao_doc TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS parte_exibicao_tipo TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS valor_liquido_correto NUMERIC")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS status_valor_liquido TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS campos_ausentes_xml TEXT")
        conn.execute("ALTER TABLE nfse_notas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT now()")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_nfse_notas_cert_chave ON nfse_notas (cert_alias, chave_nfse)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_nfse_notas_processo ON nfse_notas (processo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_nfse_notas_tipo_nota ON nfse_notas (tipo_nota)")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS nfse_processo_notas (
          processo_id UUID NOT NULL,
          nota_id BIGINT NOT NULL REFERENCES nfse_notas(id) ON DELETE CASCADE,
          created_at TIMESTAMP NOT NULL DEFAULT now(),
          PRIMARY KEY (processo_id, nota_id)
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_nfse_processo_notas_nota ON nfse_processo_notas (nota_id)")
        conn.execute(
            """
            UPDATE nfse_notas
            SET status_base_calculo = CASE
                    WHEN valor_bc IS NULL THEN 'ausente'
                    WHEN valor_bc < -0.01 THEN 'divergente'
                    WHEN valor_total IS NOT NULL AND valor_bc > valor_total + 0.01 THEN 'divergente'
                    ELSE 'ok'
                END,
                updated_at = now()
            WHERE status_base_calculo IS DISTINCT FROM CASE
                    WHEN valor_bc IS NULL THEN 'ausente'
                    WHEN valor_bc < -0.01 THEN 'divergente'
                    WHEN valor_total IS NOT NULL AND valor_bc > valor_total + 0.01 THEN 'divergente'
                    ELSE 'ok'
                END
            """
        )


def _to_text_alertas(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    if isinstance(value, list):
        partes = [str(item).strip() for item in value if item is not None and str(item).strip()]
        return " | ".join(partes) if partes else None
    if isinstance(value, dict):
        partes = []
        for k, v in value.items():
            k_txt = str(k).strip()
            v_txt = str(v).strip() if v is not None else ""
            if v_txt:
                partes.append(f"{k_txt}: {v_txt}" if k_txt else v_txt)
        return " | ".join(partes) if partes else None
    txt = str(value).strip()
    return txt or None


def _extract_row_id(row: Any) -> Optional[int]:
    if not row:
        return None
    try:
        value = row["id"]
        return int(value) if value is not None else None
    except Exception:
        pass
    try:
        value = row[0]
        return int(value) if value is not None else None
    except Exception:
        pass
    return None


def _extract_total(total_row: Any) -> int:
    if not total_row:
        return 0
    for key in ("total", 0):
        try:
            value = total_row[key]
            return int(value or 0)
        except Exception:
            pass
    try:
        return int(dict(total_row).get("total", 0) or 0)
    except Exception:
        return 0


def _to_decimal(v: Any) -> Optional[float]:
    """
    Converte valor monetário para float, tratando corretamente os formatos:
      - Formato BR com milhar: "3.821,61"  → 3821.61
      - Formato BR sem milhar: "821,61"    → 821.61
      - Formato US com ponto:  "3821.61"   → 3821.61
      - Inteiro puro:          "3821"      → 3821.0
      - Já é número:           3821.61     → 3821.61

    CORREÇÃO CRÍTICA: a versão anterior removia TODOS os pontos antes de
    trocar a vírgula, transformando "3821.61" em 382161. Agora detectamos
    o formato correto antes de converter.
    """
    if v in (None, ""):
        return None

    # Se já é numérico, retorna direto
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()

    # Remove prefixos de moeda e espaços (R$, $, etc.)
    s = re.sub(r"[R$\s]+", "", s)

    if not s:
        return None

    try:
        # Caso 1: formato BR com vírgula decimal "3.821,61" ou "821,61"
        # Identificado pela presença de vírgula
        if "," in s:
            # Remove pontos de milhar, troca vírgula por ponto
            s_clean = s.replace(".", "").replace(",", ".")
            return float(s_clean)

        # Caso 2: formato com ponto — precisamos distinguir:
        #   - Separador decimal US: "3821.61"  (ponto aparece uma vez, ≤2 dígitos após)
        #   - Separador de milhar:  "3.821"    (ponto aparece, 3 dígitos após, sem decimais)
        if "." in s:
            partes = s.split(".")
            # Se o último bloco tem exatamente 3 dígitos E há mais de um ponto
            # → é separador de milhar europeu sem decimais ex: "1.234.567"
            if len(partes) > 2 and all(len(p) == 3 for p in partes[1:]):
                return float(s.replace(".", ""))
            # Se o último bloco tem 3 dígitos e só há 1 ponto → ambíguo,
            # mas tratamos como separador de milhar (ex: "3.821" → 3821)
            # APENAS se não há casas que indiquem decimal típico (1 ou 2 dígitos)
            last = partes[-1]
            if len(partes) == 2 and len(last) == 3 and last.isdigit():
                # "3.821" → milhar → 3821
                return float(s.replace(".", ""))
            # Caso geral: ponto decimal normal "3821.61", "0.50"
            return float(s)

        # Caso 3: número inteiro puro "3821"
        return float(s)

    except Exception:
        return None


def _status_compare(xml_value: Optional[float], expected_value: Optional[float], tolerance: float = 0.01) -> str:
    if xml_value is None:
        return "ausente"
    if expected_value is None:
        return "ok"
    return "ok" if abs(xml_value - expected_value) <= tolerance else "divergente"


def _build_campos_ausentes_xml(data: dict) -> Optional[str]:
    campos_obrigatorios = [
        "N° Documento",
        "Competência",
        "Data de Emissão",
        "Município",
        "CNPJ/CPF",
        "Razão Social",
        "Valor Total",
        "Valor Líquido",
        "Valor B/C",
    ]
    faltantes = [campo for campo in campos_obrigatorios if data.get(campo) in (None, "")]
    return " | ".join(faltantes) if faltantes else None


def salvar_nota_nfse(cert_alias: str, processo_id: str | None, data: dict, arquivo_origem: str | None = None) -> str:
    tipo_nota = "tomados"

    if processo_id:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT tipo_nota FROM nfse_processos WHERE id = %s",
                (processo_id,)
            ).fetchone()
            if row:
                try:
                    tipo_nota = row["tipo_nota"] or tipo_nota
                except Exception:
                    try:
                        tipo_nota = row[0] or tipo_nota
                    except Exception:
                        pass

    nome_raw = data.get("Razão Social") or "—"
    doc_raw = data.get("CNPJ/CPF") or "—"
    parte_exibicao_tipo = "tomador" if tipo_nota == "tomados" else "prestador"
    parte_exibicao_nome = nome_raw
    parte_exibicao_doc = doc_raw

    chave_nfse = gerar_chave_nfse(data)

    valor_total   = _to_decimal(data.get("Valor Total"))
    valor_bc      = _to_decimal(data.get("Valor B/C"))
    valor_liquido = _to_decimal(data.get("Valor Líquido"))
    csrf          = _to_decimal(data.get("CSRF"))
    irrf          = _to_decimal(data.get("IRRF"))
    inss          = _to_decimal(data.get("INSS"))
    iss           = _to_decimal(data.get("ISS"))

    valor_liquido_correto = None
    if valor_total is not None:
        valor_liquido_correto = (
            valor_total
            - (csrf or 0)
            - (irrf or 0)
            - (inss or 0)
            - (iss or 0)
        )

    status_valor_liquido = _status_compare(valor_liquido, valor_liquido_correto)
    status_base_calculo  = compute_base_calculation_status(valor_bc, valor_total)

    campos_ausentes_xml  = _build_campos_ausentes_xml(data)
    alertas_fiscais_txt  = _to_text_alertas(data.get("Alertas Fiscais"))

    with get_conn() as conn:
        row = conn.execute(
            """
            INSERT INTO nfse_notas (
              cert_alias, processo_id, chave_nfse, tipo_nota, parte_exibicao_nome,
              parte_exibicao_doc, parte_exibicao_tipo,
              numero_documento, competencia, data_emissao, municipio,
              cnpj_prestador, razao_social,
              valor_total, valor_bc, valor_liquido, valor_liquido_correto,
              csrf, irrf, percentual_irrf, inss, iss,
              incidencia_iss, data_pagamento,
              codigo_servico, descricao_servico, codigo_nbs, codigo_cnae, descricao_cnae,
              simples_xml, consulta_simples_api,
              status_simples_nacional, status_csrf, status_irrf, status_inss, status_base_calculo, status_valor_liquido,
              campos_ausentes_xml, alertas_fiscais,
              dados_completos, arquivo_origem,
              updated_at
            )
            VALUES (
              %s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,
              %s,%s,
              %s,%s,%s,%s,
              %s,%s,%s,%s,%s,
              %s,%s,
              %s,%s,%s,%s,%s,
              %s,%s,
              %s,%s,%s,%s,%s,%s,
              %s,%s,
              %s,%s,
              now()
            )
            ON CONFLICT (cert_alias, chave_nfse)
            DO UPDATE SET
              tipo_nota = EXCLUDED.tipo_nota,
              parte_exibicao_nome = EXCLUDED.parte_exibicao_nome,
              parte_exibicao_doc = EXCLUDED.parte_exibicao_doc,
              parte_exibicao_tipo = EXCLUDED.parte_exibicao_tipo,
              numero_documento = EXCLUDED.numero_documento,
              competencia = EXCLUDED.competencia,
              data_emissao = EXCLUDED.data_emissao,
              municipio = EXCLUDED.municipio,
              cnpj_prestador = EXCLUDED.cnpj_prestador,
              razao_social = EXCLUDED.razao_social,
              valor_total = EXCLUDED.valor_total,
              valor_bc = EXCLUDED.valor_bc,
              valor_liquido = EXCLUDED.valor_liquido,
              valor_liquido_correto = EXCLUDED.valor_liquido_correto,
              csrf = EXCLUDED.csrf,
              irrf = EXCLUDED.irrf,
              percentual_irrf = EXCLUDED.percentual_irrf,
              inss = EXCLUDED.inss,
              iss = EXCLUDED.iss,
              incidencia_iss = EXCLUDED.incidencia_iss,
              data_pagamento = EXCLUDED.data_pagamento,
              codigo_servico = EXCLUDED.codigo_servico,
              descricao_servico = EXCLUDED.descricao_servico,
              codigo_nbs = EXCLUDED.codigo_nbs,
              codigo_cnae = EXCLUDED.codigo_cnae,
              descricao_cnae = EXCLUDED.descricao_cnae,
              simples_xml = EXCLUDED.simples_xml,
              consulta_simples_api = EXCLUDED.consulta_simples_api,
              status_simples_nacional = EXCLUDED.status_simples_nacional,
              status_csrf = EXCLUDED.status_csrf,
              status_irrf = EXCLUDED.status_irrf,
              status_inss = EXCLUDED.status_inss,
              status_base_calculo = EXCLUDED.status_base_calculo,
              status_valor_liquido = EXCLUDED.status_valor_liquido,
              campos_ausentes_xml = EXCLUDED.campos_ausentes_xml,
              alertas_fiscais = EXCLUDED.alertas_fiscais,
              dados_completos = EXCLUDED.dados_completos,
              arquivo_origem = COALESCE(EXCLUDED.arquivo_origem, nfse_notas.arquivo_origem),
              updated_at = now()
            RETURNING id
            """,
            (
                cert_alias, processo_id, chave_nfse, tipo_nota,
                parte_exibicao_nome, parte_exibicao_doc, parte_exibicao_tipo,
                data.get("N° Documento"), data.get("Competência"),
                data.get("Data de Emissão"), data.get("Município"),
                data.get("CNPJ/CPF"), data.get("Razão Social"),
                valor_total, valor_bc, valor_liquido, valor_liquido_correto,
                csrf, irrf, _to_decimal(data.get("Percentual IRRF")), inss, iss,
                data.get("Incidência do ISS"), data.get("Data do pagamento"),
                data.get("Código de serviço"), data.get("Descrição do Serviço"),
                data.get("Código NBS"), data.get("Código CNAE"), data.get("Descrição CNAE"),
                data.get("Simples Nacional / XML"), data.get("Consulta Simples API"),
                data.get("Status Simples Nacional"), data.get("Status CSRF"),
                data.get("Status IRRF"), data.get("Status INSS"),
                status_base_calculo, status_valor_liquido,
                campos_ausentes_xml, alertas_fiscais_txt,
                Jsonb(data), arquivo_origem,
            ),
        ).fetchone()

        nota_id = _extract_row_id(row)

        if row and nota_id is None:
            raise RuntimeError(f"RETURNING id veio em formato inesperado: {type(row)} / {row}")

        if processo_id and nota_id is not None:
            conn.execute(
                """
                INSERT INTO nfse_processo_notas (processo_id, nota_id)
                VALUES (%s, %s)
                ON CONFLICT (processo_id, nota_id) DO NOTHING
                """,
                (processo_id, nota_id),
            )

    return chave_nfse


def atualizar_nota_campos_editaveis(nota_id: int, valor_liquido_correto: Optional[float], alertas_fiscais: Optional[str]) -> bool:
    """
    Permite ao portal sobrescrever apenas os campos editáveis pelo auditor:
    valor_liquido_correto e alertas_fiscais.
    Recalcula status_valor_liquido automaticamente após a edição.
    """
    with get_conn() as conn:
        row = conn.execute(
            "SELECT valor_liquido, valor_liquido_correto FROM nfse_notas WHERE id = %s",
            (nota_id,)
        ).fetchone()

        if not row:
            return False

        novo_correto = valor_liquido_correto if valor_liquido_correto is not None else row["valor_liquido_correto"]
        valor_liquido = row["valor_liquido"]
        novo_status = _status_compare(valor_liquido, novo_correto)

        conn.execute(
            """
            UPDATE nfse_notas
            SET valor_liquido_correto = %s,
                alertas_fiscais = %s,
                status_valor_liquido = %s,
                updated_at = now()
            WHERE id = %s
            """,
            (novo_correto, alertas_fiscais, novo_status, nota_id),
        )
    return True


def _build_where(filters: Optional[dict], processo_id: Optional[str] = None) -> Tuple[str, List[Any]]:
    params: List[Any] = []
    where_clauses: List[str] = []

    if processo_id:
        where_clauses.append(
            "EXISTS (SELECT 1 FROM nfse_processo_notas ppn WHERE ppn.nota_id = n.id AND ppn.processo_id = %s)"
        )
        params.append(processo_id)

    if filters:
        status = filters.get("status")
        if status:
            where_clauses.append(f"{STATUS_EXPR} = %s")
            params.append(status)

        municipio = filters.get("municipio")
        if municipio:
            where_clauses.append("n.municipio ILIKE %s")
            params.append(f"%{municipio}%")

        cnpj_cpf = filters.get("cnpj_cpf")
        if cnpj_cpf:
            where_clauses.append("COALESCE(n.parte_exibicao_doc, n.cnpj_prestador, '') ILIKE %s")
            params.append(f"%{cnpj_cpf}%")

        competencia = filters.get("competencia")
        if competencia:
            where_clauses.append("n.competencia = %s")
            params.append(competencia)

        codigo_servico = filters.get("codigo_servico")
        if codigo_servico:
            where_clauses.append("n.codigo_servico = %s")
            params.append(codigo_servico)

        cert_alias = filters.get("cert_alias")
        if cert_alias:
            where_clauses.append("n.cert_alias = %s")
            params.append(cert_alias)

        if filters.get("somente_divergentes"):
            where_clauses.append(f"{STATUS_EXPR} = 'divergente'")

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return where, params


def listar_notas_por_processo(
    processo_id: str,
    filters: Optional[dict] = None,
    page: int = 1,
    page_size: int = 20,
    order_by: str = "n.created_at DESC",
) -> tuple[List[dict], int]:
    offset = (page - 1) * page_size
    where, params = _build_where(filters, processo_id=processo_id)

    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT n.id,
                   %s::uuid as processo_id,
                   COALESCE(p.cert_alias, n.cert_alias) as certificado,
                   n.tipo_nota,
                   n.numero_documento,
                   n.competencia,
                   n.municipio,
                   n.chave_nfse as chave_acesso,
                   n.data_emissao,
                   COALESCE(n.parte_exibicao_doc, n.cnpj_prestador, '—') as cnpj_cpf,
                   COALESCE(n.parte_exibicao_nome, n.razao_social, '—') as razao_social,
                   COALESCE(n.parte_exibicao_tipo, CASE WHEN n.tipo_nota = 'tomados' THEN 'tomador' ELSE 'prestador' END, '') as parte_exibicao_tipo,
                   COALESCE(n.parte_exibicao_nome, n.razao_social, '—') as parte_exibicao_nome,
                   COALESCE(n.parte_exibicao_doc, n.cnpj_prestador, '—') as parte_exibicao_doc,
                   n.valor_total,
                   n.valor_bc as valor_base,
                   n.csrf,
                   n.irrf,
                   n.percentual_irrf,
                   n.inss,
                   n.iss,
                   n.valor_liquido,
                   n.valor_liquido_correto,
                   n.status_valor_liquido,
                   {STATUS_EXPR} as status,
                   n.campos_ausentes_xml,
                   n.incidencia_iss,
                   n.data_pagamento,
                   n.codigo_servico,
                   n.descricao_servico,
                   n.codigo_nbs,
                   n.codigo_cnae as cnae,
                   n.descricao_cnae,
                   n.simples_xml as simples_nacional,
                   n.consulta_simples_api,
                   n.status_simples_nacional,
                   n.status_csrf,
                   n.status_irrf,
                   n.status_inss,
                   n.status_base_calculo,
                   n.alertas_fiscais,
                   n.created_at as dia_processado,
                   n.updated_at
            FROM nfse_notas n
            LEFT JOIN nfse_processos p ON p.id = %s
            {where}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
            """,
            [processo_id, processo_id, *params, page_size, offset],
        ).fetchall()

        total_row = conn.execute(
            f"SELECT COUNT(*) AS total FROM nfse_notas n {where}",
            params
        ).fetchone()

    return [dict(r) for r in rows], _extract_total(total_row)


def listar_notas_agrupadas(filters: Optional[dict] = None, page: int = 1, page_size: int = 200) -> tuple[List[dict], int]:
    offset = (page - 1) * page_size
    where, params = _build_where(filters)

    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT n.id,
                   COALESCE(ppn.processo_id, n.processo_id) as processo_id,
                   COALESCE(p.cert_alias, n.cert_alias) as certificado,
                   n.tipo_nota,
                   n.numero_documento,
                   n.competencia,
                   n.municipio,
                   n.chave_nfse as chave_acesso,
                   n.data_emissao,
                   COALESCE(n.parte_exibicao_doc, n.cnpj_prestador, '—') as cnpj_cpf,
                   COALESCE(n.parte_exibicao_nome, n.razao_social, '—') as razao_social,
                   COALESCE(n.parte_exibicao_tipo, '') as parte_exibicao_tipo,
                   COALESCE(n.parte_exibicao_nome, n.razao_social, '—') as parte_exibicao_nome,
                   COALESCE(n.parte_exibicao_doc, n.cnpj_prestador, '—') as parte_exibicao_doc,
                   n.valor_total,
                   n.valor_bc as valor_base,
                   n.csrf,
                   n.percentual_irrf,
                   n.valor_liquido,
                   n.valor_liquido_correto,
                   n.status_valor_liquido,
                   n.irrf,
                   n.iss,
                   n.inss,
                   {STATUS_EXPR} as status,
                   n.campos_ausentes_xml,
                   n.incidencia_iss,
                   n.data_pagamento,
                   n.codigo_servico,
                   n.descricao_servico,
                   n.codigo_nbs,
                   n.codigo_cnae as cnae,
                   n.descricao_cnae,
                   n.simples_xml as simples_nacional,
                   n.consulta_simples_api,
                   n.status_simples_nacional,
                   n.status_csrf,
                   n.status_irrf,
                   n.status_inss,
                   n.status_base_calculo,
                   n.alertas_fiscais,
                   n.created_at as dia_processado,
                   n.updated_at
            FROM nfse_notas n
            LEFT JOIN nfse_processo_notas ppn ON ppn.nota_id = n.id
            LEFT JOIN nfse_processos p ON p.id = COALESCE(ppn.processo_id, n.processo_id)
            {where}
            ORDER BY n.updated_at DESC, n.created_at DESC
            LIMIT %s OFFSET %s
            """,
            [*params, page_size, offset],
        ).fetchall()

        total_row = conn.execute(
            f"SELECT COUNT(*) AS total FROM nfse_notas n {where}",
            params
        ).fetchone()

    return [dict(r) for r in rows], _extract_total(total_row)


def obter_resumo_processo(processo_id: str) -> Dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) as total_notas,
                COALESCE(SUM(CASE WHEN {STATUS_EXPR} = 'correta' THEN 1 ELSE 0 END), 0) as total_corretas,
                COALESCE(SUM(CASE WHEN {STATUS_EXPR} = 'divergente' THEN 1 ELSE 0 END), 0) as total_divergentes,
                COALESCE(SUM(n.valor_total), 0) as valor_total_processado
            FROM nfse_notas n
            WHERE EXISTS (
                SELECT 1 FROM nfse_processo_notas ppn
                WHERE ppn.nota_id = n.id AND ppn.processo_id = %s
            )
            """,
            (processo_id,),
        ).fetchone()

    resumo = dict(row or {})
    resumo["total_notas"]              = resumo.get("total_notas") or 0
    resumo["total_corretas"]           = resumo.get("total_corretas") or 0
    resumo["total_divergentes"]        = resumo.get("total_divergentes") or 0
    resumo["valor_total_processado"]   = resumo.get("valor_total_processado") or 0
    resumo["principais_municipios"]    = []
    resumo["principais_codigos_servico"] = []
    resumo["principais_alertas"]       = []
    return resumo
