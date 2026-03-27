from __future__ import annotations

import os
import time
import random
import uuid
import glob
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .nfse_xml_converter import NFSeXMLConverterComAPI
from .playwright_downloader import executar_fluxo_nfse_playwright
from .downloader import distribuir_por_competencia, criar_estrutura_pastas
from .spreadsheet import atualizar_planilha_incremental
from .notas_repo import salvar_nota_nfse, gerar_chave_nfse, garantir_schema_nfse_notas, listar_notas_por_processo, obter_resumo_processo
from .run_state_repo import get_state, upsert_state, garantir_schema_run_state
from .processos_repo import garantir_schema_nfse_processos, criar_processo, atualizar_status_processo, atualizar_totais_processo
from .arquivos_repo import garantir_schema_nfse_processo_arquivos, registrar_arquivo_processo
from .storage import upload_pdf, upload_xml, upload_relatorio, is_s3_configured
from .execucoes_repo import garantir_schema_nfse_execucoes, criar_execucao, atualizar_status_execucao
import uuid
from datetime import datetime


@dataclass
class RunConfig:
    modo: str  # 'manual' | 'automatico'
    base_dir: str
    certs_json_path: str
    cert_aliases: list[str]
    start: date | None = None
    end: date | None = None
    headless: bool = False
    chunk_days: int = 15  # (legacy) Chunking no Python está desativado; split >800 ocorre no Node na mesma sessão
    consultar_api: bool = True
    login_type: str = "certificado"  # 'certificado' ou 'cpf_cnpj'
    credentials_json_path: str = ""  # Caminho para credentials.json
    tipo_nota: str = "tomados"  # 'tomados' (Recebidas) ou 'prestados' (Emitidas)


def _date_to_br(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _chunk_ranges(start: date, end: date, chunk_days: int):
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=chunk_days - 1))
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _resolver_intervalo_automatico(cfg: RunConfig, cert_alias: str) -> tuple[date, date]:
    """Resolve o intervalo automático para processamento.
    
    Requisito 4: Sempre processar os ÚLTIMOS 30 DIAS.
    As datas já devem estar configuradas em cfg.start e cfg.end pelo main.py.
    Esta função apenas valida e retorna os valores.
    """
    hoje = date.today()
    
    # Se cfg.start e cfg.end já estão definidos (últimos 30 dias), usar esses valores
    if cfg.start is not None and cfg.end is not None:
        start = cfg.start
        end = cfg.end
    else:
        # Fallback: calcular últimos 30 dias se não definido
        end = hoje
        start = hoje - timedelta(days=29)
    
    # Validação: não processar datas futuras
    if end > hoje:
        end = hoje
    if start > end:
        start = end
        
    return start, end


def run_processing(cfg: RunConfig, logger=None) -> None:
    """Orquestra execução manual (GUI) e automática (CLI) sem depender de Tkinter.
    
    Args:
        logger: Optional StructuredLogger for structured logging.
    """
    if logger is None:
        from worker.logging import StructuredLogger
        logger = StructuredLogger('WARNING')  # Minimal fallback

    os.makedirs(cfg.base_dir, exist_ok=True)
    garantir_schema_run_state()
    garantir_schema_nfse_notas()

    converter = NFSeXMLConverterComAPI(tipo_nota=cfg.tipo_nota, consultar_api=cfg.consultar_api)

    def _process_tmp_dir(tmp_dir: str, base_dir_cert: str, periodo_start: date, periodo_end: date) -> None:
        """Move XMLs para estrutura final, processa, persiste e atualiza planilha do *período filtrado*."""
        moved = distribuir_por_competencia(tmp_dir, base_dir_cert)
        xml_paths = moved.get('xml') or []
        if not xml_paths:
            logger.info("Nenhum XML novo para processar neste chunk.")
            return

        logger.info("Processando XMLs", {'count': len(xml_paths)})
        dados = converter.process_multiple_files(xml_paths)
        if not dados:
            logger.warning("Nenhum dado extraído dos XMLs movidos.")
            return

        dados = converter.consultar_cnpjs_em_lote(dados)

        # persistir no banco (dedupe por cert_alias+chave_nfse)
        for d in dados:
            try:
                arquivo_origem = d.get('_arquivo_origem') or d.get('_Arquivo_Origem')
                salvar_nota_nfse(cert_alias, getattr(cfg, 'processo_id', None), d, arquivo_origem=arquivo_origem)
            except Exception as save_err:
                logger.warning(f"Erro salvando nota | nota_chave={gerar_chave_nfse(d)} | arquivo={d.get('_arquivo_origem') or d.get('_Arquivo_Origem')} | erro={save_err}")

        # planilha do período (incremental, 1 arquivo por execução/período)
        # Regra: salvar SEMPRE de acordo com o período filtrado (start/end),
        # evitando "jogar" notas em meses diferentes por causa da data de emissão/competência.
        estrutura = criar_estrutura_pastas(
            base_dir_cert,
            data_referencia=datetime(periodo_end.year, periodo_end.month, 1),
        )
        planilhas_dir = estrutura['planilhas_dir']
        
        # Verifica se já existe alguma planilha na pasta para usar como base
        planilhas_existentes = glob.glob(os.path.join(planilhas_dir, "auditoria_nfse*.xlsx"))
        
        if planilhas_existentes:
            # Usa a primeira planilha existente como base (atualização incremental)
            caminho_planilha = planilhas_existentes[0]
            nome_periodo = os.path.basename(caminho_planilha).replace("auditoria_nfse_", "").replace(".xlsx", "")
            logger.info("Planilha existente encontrada", {'planilha': os.path.basename(caminho_planilha)})
        else:
            # Cria nova planilha com nome baseado no período
            nome_periodo = f"{periodo_start.isoformat()}_a_{periodo_end.isoformat()}"
            planilha_nome = f"auditoria_nfse_{cert_alias}_{nome_periodo}.xlsx"
            caminho_planilha = os.path.join(planilhas_dir, planilha_nome)

        existentes, adicionados = atualizar_planilha_incremental(converter, caminho_planilha, dados)
        logger.info("Planilha atualizada", {'periodo': nome_periodo, 'existentes': existentes, 'adicionados': adicionados, 'planilha': os.path.basename(caminho_planilha)})


        # marca progresso ok (até o fim do chunk)
        nonlocal last_ok_date
        last_ok_date = periodo_end

    for i_cert, cert_alias in enumerate(cfg.cert_aliases, start=1):
        print(f"\n{'='*60}")
        print(f"{'CREDENCIAL' if cfg.login_type == 'cpf_cnpj' else 'CERTIFICADO'}: {cert_alias}")
        print("="*60)

        base_dir_cert = os.path.join(cfg.base_dir, cert_alias)
        os.makedirs(base_dir_cert, exist_ok=True)

        if cfg.modo == 'automatico':
            start, end = _resolver_intervalo_automatico(cfg, cert_alias)
        else:
            if not cfg.start or not cfg.end:
                raise ValueError("Modo manual requer start e end")
            start, end = cfg.start, cfg.end

        upsert_state(cert_alias, status='running', last_error=None)
        last_ok_date: date | None = None

        try:
            # IMPORTANTE: Chunking no Python foi desativado.
            # Para um dado certificado e período, chamamos o Playwright apenas UMA vez.
            # Se houver >800 notas, o split é tratado INTERNAMENTE no Node, na mesma sessão (sem relogin/reabrir browser).
            print(f"\n📅 Período solicitado: {start.isoformat()} → {end.isoformat()}")
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
            tmp_dir = os.path.join(base_dir_cert, "tmp_downloads", run_id)
            os.makedirs(tmp_dir, exist_ok=True)

            ok, total_xmls, need_to_split, error_msg = executar_fluxo_nfse_playwright(
                cert_alias=cert_alias,
                data_inicial=_date_to_br(start),
                data_final=_date_to_br(end),
                diretorio_base=base_dir_cert,
                certs_json_path=cfg.certs_json_path,
                credentials_json_path=cfg.credentials_json_path,
                login_type=cfg.login_type,
                headless=cfg.headless,
                download_dir=tmp_dir,
                tipo_nota=cfg.tipo_nota,
            )
            if not ok:
                detail = f": {error_msg}" if error_msg else ""
                raise RuntimeError(f"Falha no download Playwright para {cert_alias} ({start}..{end}){detail}")

            _process_tmp_dir(tmp_dir, base_dir_cert, start, end)

            upsert_state(cert_alias, last_processed_date=last_ok_date or end, status='ok', last_error=None)

        except Exception as e:
            print(f"❌ Erro no processamento para {cert_alias}: {e}")
            upsert_state(cert_alias, status='error', last_error=str(e))
            if getattr(cfg, 'processo_id', None):
                raise
            pass

        finally:
            # Política de espera pós-certificado (sempre, inclusive em erro)
            # Não espera antes do 1º; a espera ocorre APÓS concluir cada certificado.
            try:
                n = i_cert
                sleep_s = 0.0
                if n >= 1:
                    if n <= 5:
                        sleep_s = random.uniform(180, 300)
                    elif 6 <= n <= 9:
                        base = random.uniform(180, 300)
                        extra = 60 + (n - 6) * 30
                        sleep_s = base + extra
                    else:
                        sleep_s = random.uniform(480, 540)

                    print(f"⏸ Espera pós-certificado {n}: {int(sleep_s)}s")
                    time.sleep(sleep_s)
            except Exception:
                pass
