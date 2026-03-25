from __future__ import annotations

from typing import Any, List, Optional

from .db import get_conn
from .schemas import TipoArquivoEnum, ArquivoResponse


def garantir_schema_nfse_processo_arquivos():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS nfse_processo_arquivos (
          id BIGSERIAL PRIMARY KEY,
          processo_id UUID NOT NULL REFERENCES nfse_processos(id) ON DELETE CASCADE,
          tipo_arquivo TEXT NOT NULL CHECK (tipo_arquivo IN ('pdf', 'xml', 'relatorio')),
          nome_arquivo TEXT NOT NULL,
          storage_key TEXT,
          caminho_local TEXT,
          content_type TEXT,
          tamanho_bytes BIGINT,
          competencia TEXT,
          created_at TIMESTAMP DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_nfse_arquivos_processo_tipo ON nfse_processo_arquivos (processo_id, tipo_arquivo);
        CREATE INDEX IF NOT EXISTS idx_nfse_arquivos_competencia ON nfse_processo_arquivos (competencia);
        """)


def _uuid_to_str(value: Any) -> Optional[str]:
    return str(value) if value is not None else None


def _row_to_arquivo_response(row) -> ArquivoResponse:
    data = dict(row)
    data["processo_id"] = _uuid_to_str(data.get("processo_id"))
    return ArquivoResponse(**data)


def registrar_arquivo_processo(
    processo_id: str,
    tipo: TipoArquivoEnum,
    nome_arquivo: str,
    storage_key: Optional[str] = None,
    caminho_local: Optional[str] = None,
    content_type: Optional[str] = None,
    tamanho_bytes: Optional[int] = None,
    competencia: Optional[str] = None
):
    garantir_schema_nfse_processo_arquivos()

    tipo_db = tipo.value if hasattr(tipo, "value") else str(tipo)

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO nfse_processo_arquivos (
              processo_id, tipo_arquivo, nome_arquivo, storage_key, caminho_local,
              content_type, tamanho_bytes, competencia
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            processo_id,
            tipo_db,
            nome_arquivo,
            storage_key,
            caminho_local,
            content_type,
            tamanho_bytes,
            competencia
        ))


def listar_arquivos_processo(processo_id: str, tipo: Optional[TipoArquivoEnum] = None) -> List[ArquivoResponse]:
    garantir_schema_nfse_processo_arquivos()

    params = [processo_id]
    where = ""

    if tipo:
        tipo_db = tipo.value if hasattr(tipo, "value") else str(tipo)
        where = " AND tipo_arquivo = %s"
        params.append(tipo_db)

    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT *
            FROM nfse_processo_arquivos
            WHERE processo_id = %s {where}
            ORDER BY created_at DESC
        """, params).fetchall()

    return [_row_to_arquivo_response(row) for row in rows]


def obter_arquivo_processo(arquivo_id: int) -> Optional[ArquivoResponse]:
    garantir_schema_nfse_processo_arquivos()

    with get_conn() as conn:
        row = conn.execute("""
            SELECT * FROM nfse_processo_arquivos WHERE id = %s
        """, (arquivo_id,)).fetchone()

        if not row:
            return None

        return _row_to_arquivo_response(row)