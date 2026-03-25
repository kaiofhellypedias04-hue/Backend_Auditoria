"""
Integração com MinIO/S3 compatível.

- Upload de PDFs, XMLs e relatórios após processamento
- Geração de URLs pré-assinadas para download (válidas por 1h)
- Limpeza automática de arquivos com mais de 15 dias

Otimizações:
- Pool de clientes S3 por thread (thread-safe)
- Configurações em cache (lidas uma única vez)
- put_object direto em vez de upload_file (menos overhead)
- TransferConfig ajustado para arquivos pequenos (PDFs/XMLs de NFS-e)
"""
import os
import io
import threading
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

RETENCAO_DIAS = 15

# ── Cache de configurações (lido uma vez, não a cada upload) ──────────────────
_settings_cache: Optional[dict] = None
_settings_lock = threading.Lock()

def get_s3_settings() -> dict:
    global _settings_cache
    if _settings_cache is None:
        with _settings_lock:
            if _settings_cache is None:
                _settings_cache = {
                    "endpoint":   os.getenv("S3_ENDPOINT"),
                    "bucket":     os.getenv("S3_BUCKET"),
                    "access_key": os.getenv("S3_ACCESS_KEY"),
                    "secret_key": os.getenv("S3_SECRET_KEY"),
                    "region":     os.getenv("S3_REGION", "us-east-1"),
                }
    return _settings_cache


def is_s3_configured() -> bool:
    s = get_s3_settings()
    return all([s["endpoint"], s["bucket"], s["access_key"], s["secret_key"]])


# ── Pool de clientes por thread (cada thread tem seu próprio cliente) ─────────
_thread_local = threading.local()

def get_s3_client():
    """Retorna um cliente S3 exclusivo para a thread atual — thread-safe."""
    if not hasattr(_thread_local, "client"):
        s = get_s3_settings()
        _thread_local.client = boto3.client(
            "s3",
            endpoint_url=s["endpoint"],
            aws_access_key_id=s["access_key"],
            aws_secret_access_key=s["secret_key"],
            region_name=s["region"],
            config=Config(
                signature_version="s3v4",
                max_pool_connections=1,       # 1 por thread é suficiente
                connect_timeout=10,
                read_timeout=30,
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )
    return _thread_local.client


# TransferConfig para arquivos pequenos (PDFs/XMLs de NFS-e são < 2MB geralmente)
# Desativa multipart (overhead desnecessário para arquivos pequenos)
_transfer_config = TransferConfig(
    multipart_threshold=10 * 1024 * 1024,  # só usa multipart acima de 10MB
    max_concurrency=1,                      # 1 por thread (paralelismo já é externo)
    use_threads=False,                      # threads gerenciadas pelo runner_processos
)


def upload_file(
    file_path: str,
    storage_key: str,
    content_type: Optional[str] = None,
) -> Optional[str]:
    if not is_s3_configured():
        return None

    s3     = get_s3_client()
    bucket = get_s3_settings()["bucket"]
    path   = Path(file_path)

    try:
        # put_object com bytes em memória: mais rápido que upload_file para arquivos < 10MB
        dados = path.read_bytes()
        kwargs = {"Bucket": bucket, "Key": storage_key, "Body": dados}
        if content_type:
            kwargs["ContentType"] = content_type

        s3.put_object(**kwargs)
        logger.debug(f"[MinIO] Upload OK: {storage_key} ({len(dados)/1024:.0f} KB)")
        return storage_key
    except Exception as e:
        logger.error(f"[MinIO] Erro no upload de {file_path}: {e}")
        return None


def upload_xml(file_path: str, storage_key: str) -> Optional[str]:
    return upload_file(file_path, storage_key, "application/xml")


def upload_pdf(file_path: str, storage_key: str) -> Optional[str]:
    return upload_file(file_path, storage_key, "application/pdf")


def upload_relatorio(file_path: str, storage_key: str) -> Optional[str]:
    return upload_file(
        file_path, storage_key,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def generate_presigned_download_url(
    storage_key: str,
    expires_in: int = 3600,
) -> Optional[str]:
    if not is_s3_configured():
        return None
    s3 = get_s3_client()
    s = get_s3_settings()
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": s["bucket"], "Key": storage_key},
            ExpiresIn=expires_in,
        )
    except Exception as e:
        logger.error(f"[MinIO] Erro ao gerar URL pré-assinada para {storage_key}: {e}")
        return None


def get_local_file_path(caminho_local: str) -> Path:
    path = Path(caminho_local)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo local não encontrado: {caminho_local}")
    return path


# ─── Limpeza automática de arquivos antigos no MinIO ─────────────────────────

def limpar_arquivos_antigos_minio(dias: int = RETENCAO_DIAS) -> dict:
    """
    Remove do MinIO todos os objetos com mais de `dias` dias.
    Também atualiza o banco marcando storage_key = NULL nos registros
    cujos arquivos foram removidos.

    Retorna um dict com { "removidos": int, "erros": int, "prefixos_verificados": list }
    """
    if not is_s3_configured():
        logger.info("[MinIO] S3 não configurado — limpeza ignorada.")
        return {"removidos": 0, "erros": 0}

    s3 = get_s3_client()
    s = get_s3_settings()
    bucket = s["bucket"]
    corte = datetime.now(timezone.utc) - timedelta(days=dias)

    removidos = 0
    erros = 0
    chaves_removidas = []

    logger.info(f"[MinIO] Iniciando limpeza de arquivos anteriores a {corte.strftime('%d/%m/%Y %H:%M UTC')}")

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                last_modified = obj["LastModified"]
                # Garante que last_modified tem timezone
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=timezone.utc)

                if last_modified < corte:
                    try:
                        s3.delete_object(Bucket=bucket, Key=obj["Key"])
                        chaves_removidas.append(obj["Key"])
                        removidos += 1
                        logger.debug(f"[MinIO] Removido: {obj['Key']}")
                    except ClientError as e:
                        erros += 1
                        logger.error(f"[MinIO] Erro ao remover {obj['Key']}: {e}")

    except Exception as e:
        logger.error(f"[MinIO] Erro ao listar objetos para limpeza: {e}")

    # Atualizar banco: marcar storage_key = NULL para arquivos removidos
    if chaves_removidas:
        try:
            from .db import get_conn
            with get_conn() as conn:
                for chave in chaves_removidas:
                    conn.execute(
                        "UPDATE nfse_processo_arquivos SET storage_key = NULL WHERE storage_key = %s",
                        (chave,),
                    )
            logger.info(f"[MinIO] Banco atualizado: {len(chaves_removidas)} registros marcados sem storage_key")
        except Exception as e:
            logger.warning(f"[MinIO] Não foi possível atualizar banco após limpeza: {e}")

    logger.info(f"[MinIO] Limpeza concluída: {removidos} removidos, {erros} erros")
    return {"removidos": removidos, "erros": erros, "chaves": chaves_removidas}