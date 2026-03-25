from typing import Optional, List, Dict, Any
from datetime import date, datetime
from pydantic import BaseModel
from enum import Enum


class StatusEnum(str, Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"


class LoginTypeEnum(str, Enum):
    certificado = "certificado"
    cpf_cnpj = "cpf_cnpj"


class TipoNotaEnum(str, Enum):
    tomados = "tomados"
    prestados = "prestados"


class TipoArquivoEnum(str, Enum):
    pdf = "pdf"
    xml = "xml"
    relatorio = "relatorio"


class Pagination(BaseModel):
    items: List[Dict[str, Any]] = []
    total: int = 0
    page: int = 1
    page_size: int = 20


class ProcessoCreate(BaseModel):
    execution_id: str
    cert_alias: str
    login_type: LoginTypeEnum
    tipo_nota: TipoNotaEnum
    start_date: date
    end_date: date


class ProcessoResponse(BaseModel):
    id: str
    execution_id: str
    cert_alias: str
    login_type: str
    tipo_nota: str
    start_date: date
    end_date: date
    status: StatusEnum
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    total_notas: int = 0
    total_xml: int = 0
    total_pdf: int = 0
    total_corretas: int = 0
    total_divergentes: int = 0
    error_message: Optional[str] = None


class ArquivoResponse(BaseModel):
    id: int
    processo_id: str
    tipo_arquivo: TipoArquivoEnum
    nome_arquivo: str
    storage_key: Optional[str]
    caminho_local: Optional[str] = None
    content_type: Optional[str]
    tamanho_bytes: Optional[int]
    competencia: Optional[str]
    created_at: datetime


class NotaReportFilters(BaseModel):
    status: Optional[str] = None
    municipio: Optional[str] = None
    cnpj_cpf: Optional[str] = None
    competencia: Optional[str] = None
    codigo_servico: Optional[str] = None
    somente_divergentes: bool = False


class NotaReportRow(BaseModel):
    processo_id: Optional[str] = None
    certificado: Optional[str] = None
    tipo_nota: Optional[str] = None
    parte_exibicao_nome: Optional[str] = None
    parte_exibicao_doc: Optional[str] = None
    parte_exibicao_tipo: Optional[str] = None
    competencia: Optional[str] = None
    municipio: Optional[str] = None
    chave_acesso: Optional[str] = None
    data_emissao: Optional[date] = None
    cnpj_cpf: Optional[str] = None
    razao_social: Optional[str] = None
    numero_documento: Optional[str] = None
    valor_total: Optional[float] = None
    valor_base: Optional[float] = None
    csrf: Optional[float] = None
    irrf: Optional[float] = None
    inss: Optional[float] = None
    iss: Optional[float] = None
    valor_liquido: Optional[float] = None
    valor_liquido_correto: Optional[float] = None
    status_valor_liquido: Optional[str] = None
    status: Optional[str] = None
    campos_ausentes_xml: Optional[str] = None
    incidencia_iss: Optional[str] = None
    codigo_servico: Optional[str] = None
    codigo_nbs: Optional[str] = None
    cnae: Optional[str] = None
    simples_nacional: Optional[str] = None
    alertas_fiscais: Optional[str] = None
    dia_processado: Optional[datetime] = None


class SummaryResponse(BaseModel):
    total_notas: int
    total_corretas: int
    total_divergentes: int
    valor_total_processado: float
    principais_municipios: List[Dict[str, Any]] = []
    principais_codigos_servico: List[Dict[str, Any]] = []
    principais_alertas: List[Dict[str, Any]] = []