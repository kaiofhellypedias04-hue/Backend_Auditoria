from __future__ import annotations
import os
import shutil
import logging
from pathlib import Path
from datetime import date
from typing import Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

logger_cleanup = logging.getLogger("cleanup")

from .runner import RunConfig, run_processing as run_processing_without_process
from .processos_repo import atualizar_status_processo, atualizar_totais_processo, garantir_schema_nfse_processos
from .execucoes_repo import atualizar_status_execucao, garantir_schema_nfse_execucoes
from .arquivos_repo import garantir_schema_nfse_processo_arquivos
from .storage import upload_pdf, upload_xml, upload_relatorio, is_s3_configured
from .notas_repo import obter_resumo_processo, garantir_schema_nfse_notas
from .schemas import LoginTypeEnum, TipoNotaEnum, StatusEnum, TipoArquivoEnum
from .db import get_conn
from datetime import datetime
import traceback

@dataclass
class ProcessRunConfig(RunConfig):
    execution_id: str = ""
    processo_id: str = ""


def run_processing(cfg: RunConfig, logger=None, execution_id: Optional[str] = None, processo_id: Optional[str] = None):
    """Wrapper for backward compat + new process integration.
    
    If execution_id/processo_id provided, integrate DB tracking.
    """
    if execution_id and processo_id:
        return run_with_process(cfg, execution_id, processo_id, logger)
    else:
        # Legacy call
        return run_processing_without_process(cfg, logger)


def run_with_process(cfg: ProcessRunConfig, logger=None):
    garantir_schema_nfse_processos()
    garantir_schema_nfse_processo_arquivos()
    garantir_schema_nfse_notas()
    
    # Update status running
    atualizar_status_processo(cfg.processo_id, StatusEnum.running, datetime.now())
    atualizar_status_execucao(cfg.execution_id, 'running', datetime.now())
    
    try:
        # Run core logic (download, convert, save notes with processo_id)
        run_processing_without_process(cfg, logger)
        
        # Post-process: get totals from DB, register files and get file counts
        resumo = obter_resumo_processo(cfg.processo_id)
        total_notas = resumo.get('total_notas', 0)
        total_corretas = resumo.get('total_corretas', 0)
        total_divergentes = resumo.get('total_divergentes', 0)

        # 1. Faz upload de todos os arquivos para o MinIO e registra no banco
        num_xml, num_pdf = register_process_files(cfg)
        atualizar_totais_processo(cfg.processo_id, total_notas, num_xml, num_pdf, total_corretas, total_divergentes)

        # 2. Somente após confirmar que tudo subiu, apaga a pasta local
        resultado_cleanup = limpar_pasta_local(cfg)
        if resultado_cleanup.get("limpo"):
            logger_cleanup.info(
                f"[Cleanup] Processo {cfg.processo_id} — pasta local removida. "
                f"Arquivos: {resultado_cleanup.get('arquivos_confirmados')}. "
                f"Pastas: {resultado_cleanup.get('pastas_removidas')}"
            )
        else:
            logger_cleanup.warning(
                f"[Cleanup] Processo {cfg.processo_id} — pasta local mantida. "
                f"Motivo: {resultado_cleanup.get('motivo')}"
            )

        atualizar_status_processo(cfg.processo_id, StatusEnum.completed, finished_at=datetime.now())
        atualizar_status_execucao(cfg.execution_id, 'completed', finished_at=datetime.now())
        
    except Exception as e:
        aliases = ", ".join(cfg.cert_aliases or [])
        error_message = f"Processo {cfg.processo_id} falhou para [{aliases}]: {e}"
        logger_cleanup.exception("[Processo] Falha na execucao %s / processo %s", cfg.execution_id, cfg.processo_id)
        atualizar_status_processo(cfg.processo_id, StatusEnum.failed, finished_at=datetime.now(), error_message=error_message)
        atualizar_status_execucao(cfg.execution_id, 'failed', finished_at=datetime.now(), error=error_message, traceback=traceback.format_exc())
        raise RuntimeError(error_message) from e


def _apenas_upload(tarefa: dict) -> dict:
    """
    Faz SOMENTE o upload para o MinIO — sem tocar no banco.
    Executado em thread paralela. Retorna dict com resultado + storage_key.
    """
    tipo_map = {
        "pdf":       upload_pdf,
        "xml":       upload_xml,
        "relatorio": upload_relatorio,
    }
    fn_upload  = tipo_map[tarefa["tipo"]]
    storage_key = fn_upload(str(tarefa["path"]), tarefa["storage_key"])
    return {
        "tipo":        tarefa["tipo"],
        "path":        tarefa["path"],
        "storage_key": storage_key,          # None se falhou
        "processo_id": tarefa["processo_id"],
        "nome":        tarefa["path"].name,
        "tamanho":     tarefa["path"].stat().st_size,
        "ok":          storage_key is not None,
    }


def register_process_files(cfg: RunConfig) -> tuple[int, int]:
    """
    1. Coleta todos os arquivos
    2. Faz upload em paralelo pro MinIO (sem DB — threads livres)
    3. Registra todos no banco em UMA única conexão em lote
    Retorna (xml_count, pdf_count).
    """
    CONTENT_TYPES = {
        "pdf":       "application/pdf",
        "xml":       "application/xml",
        "relatorio": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    ENUM_TIPOS = {
        "pdf":       TipoArquivoEnum.pdf,
        "xml":       TipoArquivoEnum.xml,
        "relatorio": TipoArquivoEnum.relatorio,
    }

    # ── 1. Coleta tarefas ────────────────────────────────────────────────────
    tarefas = []
    for cert_alias in cfg.cert_aliases:
        base = Path(cfg.base_dir) / cert_alias
        for p in base.rglob("*.pdf"):
            tarefas.append({"tipo": "pdf",       "path": p, "storage_key": f"processos/{cfg.processo_id}/{p.name}", "processo_id": cfg.processo_id})
        for p in base.rglob("*.xml"):
            tarefas.append({"tipo": "xml",       "path": p, "storage_key": f"processos/{cfg.processo_id}/{p.name}", "processo_id": cfg.processo_id})
        for p in base.rglob("auditoria_nfse*.xlsx"):
            tarefas.append({"tipo": "relatorio", "path": p, "storage_key": f"processos/{cfg.processo_id}/{p.name}", "processo_id": cfg.processo_id})

    if not tarefas:
        return 0, 0

    # ── 2. Upload paralelo pro MinIO (sem banco, sem lock) ───────────────────
    MAX_WORKERS = min(20, len(tarefas))
    logger_cleanup.info(f"[Upload] {len(tarefas)} arquivos → MinIO com {MAX_WORKERS} threads...")

    resultados = []
    erros      = []
    concluidos = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_apenas_upload, t): t for t in tarefas}
        for future in as_completed(futures):
            try:
                res = future.result()
                resultados.append(res)
                concluidos += 1
                if not res["ok"]:
                    erros.append(res["nome"])
                if concluidos % 50 == 0 or concluidos == len(tarefas):
                    logger_cleanup.info(f"[Upload] {concluidos}/{len(tarefas)} enviados ao MinIO...")
            except Exception as e:
                erros.append(str(e))
                logger_cleanup.error(f"[Upload] Erro: {e}")

    if erros:
        logger_cleanup.warning(f"[Upload] {len(erros)} falha(s): {erros[:5]}")

    # ── 3. Registra tudo no banco em UMA única conexão ───────────────────────
    pdf_count = 0
    xml_count = 0

    with get_conn() as conn:
        for res in resultados:
            try:
                conn.execute("""
                    INSERT INTO nfse_processo_arquivos
                        (processo_id, tipo_arquivo, nome_arquivo, storage_key,
                         caminho_local, content_type, tamanho_bytes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    res["processo_id"],
                    res["tipo"],
                    res["nome"],
                    res["storage_key"],
                    str(res["path"]),
                    CONTENT_TYPES[res["tipo"]],
                    res["tamanho"],
                ))
                if res["tipo"] == "pdf":
                    pdf_count += 1
                elif res["tipo"] == "xml":
                    xml_count += 1
            except Exception as e:
                logger_cleanup.error(f"[Upload] Erro ao registrar {res['nome']} no banco: {e}")

    logger_cleanup.info(f"[Upload] Concluído: {pdf_count} PDFs + {xml_count} XMLs registrados no banco")
    return xml_count, pdf_count


def limpar_pasta_local(cfg: RunConfig) -> dict:
    """
    Apaga a pasta local do processo (base_dir/cert_alias) SOMENTE se:
      1. O MinIO estiver configurado
      2. Todos os arquivos tiverem storage_key (confirmando que foram enviados)

    Retorna um dict com o resultado da limpeza.
    """
    if not is_s3_configured():
        logger_cleanup.info("[Cleanup] MinIO não configurado — pasta local mantida.")
        return {"limpo": False, "motivo": "minio_nao_configurado"}

    # Verifica no banco se todos os arquivos do processo têm storage_key
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN storage_key IS NOT NULL THEN 1 ELSE 0 END) as enviados
                   FROM nfse_processo_arquivos
                   WHERE processo_id = %s""",
                (cfg.processo_id,)
            ).fetchone()
        total   = rows["total"]   if rows else 0
        enviados = rows["enviados"] if rows else 0
    except Exception as e:
        logger_cleanup.warning(f"[Cleanup] Erro ao verificar banco: {e}")
        return {"limpo": False, "motivo": f"erro_banco: {e}"}

    if total == 0:
        logger_cleanup.info("[Cleanup] Nenhum arquivo registrado — pasta local mantida.")
        return {"limpo": False, "motivo": "sem_arquivos"}

    if enviados < total:
        logger_cleanup.warning(
            f"[Cleanup] Apenas {enviados}/{total} arquivos enviados ao MinIO. "
            "Pasta local NÃO será apagada."
        )
        return {"limpo": False, "motivo": f"upload_incompleto_{enviados}/{total}"}

    # Todos enviados — pode apagar
    pastas_removidas = []
    erros = []
    for cert_alias in cfg.cert_aliases:
        pasta = Path(cfg.base_dir) / cert_alias
        if pasta.exists():
            try:
                shutil.rmtree(pasta)
                pastas_removidas.append(str(pasta))
                logger_cleanup.info(f"[Cleanup] Pasta removida: {pasta}")
            except Exception as e:
                erros.append(str(e))
                logger_cleanup.error(f"[Cleanup] Erro ao remover {pasta}: {e}")

    return {
        "limpo": len(pastas_removidas) > 0,
        "pastas_removidas": pastas_removidas,
        "erros": erros,
        "arquivos_confirmados": f"{enviados}/{total}",
    }

# Note: run_processing_without_process is original run_processing renamed after refactor
