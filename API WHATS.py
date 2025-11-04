import os
import glob
import json
import time
import logging
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import pandas as pd
import requests
import pytz

# ------------- CONFIG -------------
WATCH_FOLDER = Path(r"") #PASTA DE ARQUIVOS
PENDING_FOLD = Path(r"") # Armazenamento JSON
#WATCH_FOLDER = Path("/home/carlos/source/repos/lab/crc_zap/data/")
 
FINAL_FOLDER = WATCH_FOLDER / "FINAL"
FINAL_FOLDER.mkdir(exist_ok=True, parents=True)

# Arquivo para armazenar ações em processamento
ACOES_DB_FILE = PENDING_FOLD / "acoes_pendentes.json"

# UNO API config
UNO_BASE = "https://uno-portal-api.contactvoice.com.br"
UNO_LOGIN_ENDPOINT = "/Login/login"
UNO_INCLUIR_ENDPOINT = "/Uno/IncluirAcaoEnvio"
UNO_GET_RETORNO = "/Uno/GetAcaoEnvioRetorno"

# Credenciais de login
UNO_LOGIN_EMAIL = "" #E-mail de acesso a API
UNO_LOGIN_SENHA = "" #Senha da API
UNO_ID_EMPRESA = 90

# Timezone (São Paulo - America/Sao_Paulo)
TIMEZONE = pytz.timezone('America/Sao_Paulo')

# Token global (será atualizado pelo login)
UNO_AUTH_BEARER = None
TOKEN_EXPIRY = None

# Timings
LOOP_SECONDS = 30
POLL_SECONDS = 10
POLL_MAX_ATTEMPTS = 3  # Tentativas antes de deixar em background
POST_TIMEOUT = 60
GET_TIMEOUT = 30
MAX_RETRIES_HTTP = 3
TOKEN_REFRESH_MARGIN = timedelta(minutes=5)  # Renovar token 5min antes de expirar

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------- helpers -------------
def limpar_tela():
    os.system('cls' if os.name == 'nt' else 'clear')


def load_acoes_db() -> Dict[str, dict]:
    """Carrega o banco de dados de ações pendentes"""
    if not ACOES_DB_FILE.exists():
        return {}
    try:
        with open(ACOES_DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Erro ao carregar banco de ações: {e}")
        return {}


def save_acoes_db(acoes: Dict[str, dict]):
    """Salva o banco de dados de ações pendentes"""
    try:
        with open(ACOES_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(acoes, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Erro ao salvar banco de ações: {e}")


def add_acao_pendente(id_acao: int, file_path: Path, centro_custo: str):
    """Adiciona uma ação pendente ao banco de dados"""
    acoes = load_acoes_db()
    acoes[str(id_acao)] = {
        "idAcaoEnvio": id_acao,
        "arquivo_original": str(file_path),
        "arquivo_nome": file_path.name,
        "centro_custo": centro_custo,
        "data_criacao": datetime.now().isoformat(),
        "status": "pendente",
        "tentativas": 0,
        "ultima_verificacao": None
    }
    save_acoes_db(acoes)
    logging.info(f"Ação {id_acao} adicionada ao banco de dados pendentes")


def update_acao_status(id_acao: int, status: str, **kwargs):
    """Atualiza o status de uma ação"""
    acoes = load_acoes_db()
    if str(id_acao) in acoes:
        acoes[str(id_acao)]["status"] = status
        acoes[str(id_acao)]["ultima_verificacao"] = datetime.now().isoformat()
        for key, value in kwargs.items():
            acoes[str(id_acao)][key] = value
        save_acoes_db(acoes)


def remove_acao_pendente(id_acao: int):
    """Remove uma ação do banco de dados"""
    acoes = load_acoes_db()
    if str(id_acao) in acoes:
        del acoes[str(id_acao)]
        save_acoes_db(acoes)
        logging.info(f"Ação {id_acao} removida do banco de dados")


def try_read_csv(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path, sep=";", dtype=str, keep_default_na=False, na_values=[""])
    except Exception:
        try:
            return pd.read_csv(path, sep=";", dtype=str, encoding="latin-1", keep_default_na=False, na_values=[""])
        except Exception as e:
            logging.error(f"Erro lendo {path}: {e}")
            return None

def normalize_phone_raw(s: Optional[str]) -> Optional[str]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    st = str(s).strip()
    digits = "".join(ch for ch in st if ch.isdigit())
    return digits if digits else None

def determine_tem_zap_from_item(item: dict) -> str:
    try:
        status = str(item.get("statusRetornoEnvio", "") or "").upper()
        msg = str(item.get("mensagem", "") or "").upper()
        id_status = item.get("idStatusRetornoEnvio")
    except Exception:
        return "NAO"
    if "VALID" in status:
        return "SIM"
    if "WHATSAPP VALIDO" in msg or "VALIDO" in msg:
        return "SIM"
    try:
        if id_status is not None and int(id_status) == 7:
            return "SIM"
    except Exception:
        pass
    return "NAO"

def http_post_with_retry(url, params=None, files=None, headers=None, timeout=POST_TIMEOUT, json_data=None):
    last_exc = None
    for attempt in range(1, MAX_RETRIES_HTTP + 1):
        try:
            resp = requests.post(url, params=params, files=files, headers=headers, json=json_data, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            logging.warning(f"POST attempt {attempt} falhou: {e}")
            time.sleep(1 * attempt)
    raise last_exc

def http_get_with_retry(url, params=None, headers=None, timeout=GET_TIMEOUT):
    last_exc = None
    for attempt in range(1, MAX_RETRIES_HTTP + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            logging.warning(f"GET attempt {attempt} falhou: {e}")
            time.sleep(1 * attempt)
    raise last_exc


# ------------- Autenticação -------------
def fazer_login() -> Optional[str]:
    """
    Realiza login na API UNO e retorna o token Bearer.
    Atualiza as variáveis globais UNO_AUTH_BEARER e TOKEN_EXPIRY.
    """
    global UNO_AUTH_BEARER, TOKEN_EXPIRY
    
    url = UNO_BASE.rstrip("/") + UNO_LOGIN_ENDPOINT
    payload = {
        "email": UNO_LOGIN_EMAIL,
        "senha": UNO_LOGIN_SENHA
    }
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        logging.info(f"Fazendo login com e-mail: {UNO_LOGIN_EMAIL}")
        resp = http_post_with_retry(url, headers=headers, json_data=payload)
        data = resp.json()
        
        # O token pode vir em diferentes formatos de resposta
        token = data.get("token") or data.get("access_token") or data.get("bearer")
        
        if not token:
            # Se o token não vier no JSON, pode estar nos headers
            auth_header = resp.headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.replace("Bearer ", "")
        
        if token:
            UNO_AUTH_BEARER = token
            # Token JWT geralmente expira em 1 hora, vamos assumir isso
            TOKEN_EXPIRY = datetime.now() + timedelta(hours=1)
            logging.info("Login realizado com sucesso! Token obtido.")
            return token
        else:
            logging.error(f"Token não encontrado na resposta: {data}")
            return None
            
    except Exception as e:
        logging.exception(f"Erro ao fazer login: {e}")
        return None


def verificar_renovar_token() -> bool:
    """
    Verifica se o token precisa ser renovado e renova se necessário.
    Retorna True se o token está válido, False caso contrário.
    """
    global UNO_AUTH_BEARER, TOKEN_EXPIRY
    
    # Se não temos token ou não temos data de expiração, fazer login
    if not UNO_AUTH_BEARER or not TOKEN_EXPIRY:
        return fazer_login() is not None
    
    # Se o token está próximo de expirar, renovar
    if datetime.now() + TOKEN_REFRESH_MARGIN >= TOKEN_EXPIRY:
        logging.info("Token próximo de expirar, renovando...")
        return fazer_login() is not None
    
    return True

# ------------- UNO helpers -------------
def post_incluir_acao_envio(file_path: Path, centro_custo: str, email: str, id_empresa: int, token: Optional[str]=None):
    url = UNO_BASE.rstrip("/") + UNO_INCLUIR_ENDPOINT

    # Montar os parâmetros exatamente como no curl, adicionando Mensagem e ProcessamentoExterno
    # Usar horário local de São Paulo ao invés de UTC
    data_hora_sp = datetime.now(TIMEZONE).strftime("%Y-%m-%dT%H:%M:%S")
    
    params = {
        "Email": email,
        "IdEmpresa": int(id_empresa),
        "CentroCusto": centro_custo or "",
        "DataEnvio": data_hora_sp,
        "Higienizacao": "true",
        "Oficial": "false",
        "IdTipoAcaoEnvio": 1,
        "Mensagem": "Validação",
        "ProcessamentoExterno": "false"
    }
    # Atentar para o header Content-Type, deixar o requests controlar isso por causa do multipart
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    # NÃO definir Content-Type: multipart/form-data manualmente

    with open(file_path, "rb") as fh:
        files = {"Mailing": (file_path.name, fh, "text/csv")}
        resp = http_post_with_retry(url, params=params, files=files, headers=headers)
    return resp.json()

def get_acao_envio_retorno(email: str, id_acao_envio: int, token: Optional[str]=None):
    url = UNO_BASE.rstrip("/") + UNO_GET_RETORNO
    params = {"Email": email, "IdAcaoEnvio": int(id_acao_envio)}
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = http_get_with_retry(url, params=params, headers=headers)
    return resp.json()

# ------------- core processing -------------
def incluir_arquivo_para_validacao(file_path: Path):
    """
    FASE 1: Envia um arquivo CSV para validação na API UNO.
    Apenas inclui a ação e armazena no banco de dados pendentes.
    NÃO aguarda o resultado - isso será feito na fase 2.

    Após enviar com sucesso, o arquivo original é REMOVIDO da pasta WATCH_FOLDER
    para evitar reenvios.
    """
    logging.info(f"📤 Incluindo arquivo: {file_path.name}")
    
    # Verificar e renovar token antes de processar
    if not verificar_renovar_token():
        logging.error("Falha ao obter/renovar token. Não é possível processar arquivo.")
        return {"file": str(file_path), "error": "auth_failed"}
    
    df = try_read_csv(file_path)
    if df is None:
        return {"file": str(file_path), "error": "read_failed"}

    # normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    dest_col = next((c for c in df.columns if c.strip().upper() == "DESTINATARIO"), None)
    if dest_col is None:
        logging.error(f"Arquivo {file_path.name} não tem coluna 'Destinatario'.")
        return {"file": str(file_path), "error": "missing Destinatario"}

    # Var1 -> CentroCusto
    var1_col = next((c for c in df.columns if c.strip().upper() == "VAR1"), None)
    centro_custo = ""
    if var1_col:
        vals = df[var1_col].astype(str).replace("", pd.NA).dropna()
        if not vals.empty:
            centro_custo = str(vals.iloc[0]).strip()
        df[var1_col] = pd.NA

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(tmp_fd)
    tmp_file = Path(tmp_path)
    df.to_csv(tmp_file, sep=";", index=False, header=True, encoding="utf-8")

    # POST para incluir ação
    try:
        resp_json = post_incluir_acao_envio(
            file_path=tmp_file, 
            centro_custo=centro_custo, 
            email=UNO_LOGIN_EMAIL, 
            id_empresa=UNO_ID_EMPRESA, 
            token=UNO_AUTH_BEARER
        )
    except Exception as e:
        logging.exception(f"❌ POST falhou para {file_path.name}: {e}")
        tmp_file.unlink(missing_ok=True)
        return {"file": str(file_path), "error": f"post_error:{e}"}

    logging.info(f"📨 POST retorno para {file_path.name}: {resp_json}")
    id_acao = resp_json.get("idAcaoEnvio") or resp_json.get("idAcao") or resp_json.get("id")
    
    if id_acao is None:
        logging.error(f"❌ Nenhum idAcaoEnvio retornado para {file_path.name}: {resp_json}")
        tmp_file.unlink(missing_ok=True)
        return {"file": str(file_path), "error": "no_idAcaoEnvio"}

    # Adicionar ação ao banco de dados pendentes
    add_acao_pendente(id_acao, file_path, centro_custo)
    
    # === REMOVER O ARQUIVO ORIGINAL da pasta WATCH_FOLDER para evitar reenvio ===
    arquivo_movido_flag = False
    try:
        file_path.unlink(missing_ok=False)  # se falhar, será lançada exceção
        arquivo_movido_flag = True
        logging.info(f"🗑️ Arquivo original removido: {file_path.name}")
    except FileNotFoundError:
        # já não existia (ou foi movido manualmente); apenas log
        arquivo_movido_flag = False
        logging.warning(f"⚠️ Arquivo original não encontrado ao tentar remover: {file_path}")
    except Exception as e:
        arquivo_movido_flag = False
        logging.exception(f"❌ Falha ao remover arquivo original {file_path}: {e}")

    # Cleanup temp
    tmp_file.unlink(missing_ok=True)
    
    logging.info(f"✅ Arquivo {file_path.name} enviado com sucesso! ID Ação: {id_acao}")
    return {
        "file": str(file_path), 
        "idAcaoEnvio": id_acao, 
        "arquivo_movido": arquivo_movido_flag,
        "arquivo_original": str(file_path),
        "status": "enviado"
    }

def verificar_resultado_acao(id_acao: int) -> Optional[dict]:
    """
    FASE 2: Verifica se uma ação está pronta e processa o resultado.
    Faz apenas UMA tentativa por chamada - não fica bloqueado esperando.
    Retorna o resultado se estiver pronto, None caso contrário.
    """
    acoes = load_acoes_db()
    acao_info = acoes.get(str(id_acao))
    
    if not acao_info:
        logging.warning(f"⚠️  Ação {id_acao} não encontrada no banco de dados")
        return None
    
    file_path = Path(acao_info["arquivo_original"])
    tentativas_anteriores = acao_info.get("tentativas", 0)
    
    try:
        # Verificar token antes de consultar
        if not verificar_renovar_token():
            logging.error("Token inválido, abortando verificação")
            return None
        
        # Fazer GET para obter resultado
        get_resp = get_acao_envio_retorno(
            email=UNO_LOGIN_EMAIL, 
            id_acao_envio=int(id_acao), 
            token=UNO_AUTH_BEARER
        )
        
        # Extrair itens da resposta
        items = []
        if isinstance(get_resp, list) and len(get_resp) > 0:
            items = get_resp
        elif isinstance(get_resp, dict):
            items = next((v for v in get_resp.values() if isinstance(v, list) and v), [])
        
        # Verificar se todos os itens foram processados
        if items and len(items) > 0:
            # Verificar o status do primeiro item para determinar se está pronto
            primeiro_item = items[0]
            status_retorno = primeiro_item.get("statusRetornoEnvio", "")
            
            # Se pelo menos um item foi validado, consideramos que está pronto
            if status_retorno in ["Validado", "Processada", "Enviado"]:
                logging.info(f"✅ Ação {id_acao} está pronta! Status: {status_retorno}")
                resultado = processar_resultado_acao(id_acao, items, file_path)
                return resultado
            else:
                # Ainda está processando
                logging.debug(f"⏳ Ação {id_acao} ainda processando (status: {status_retorno})")
                update_acao_status(id_acao, "processando", tentativas=tentativas_anteriores + 1)
        else:
            # Sem dados ainda
            logging.debug(f"⏳ Ação {id_acao} sem dados ainda (tentativa #{tentativas_anteriores + 1})")
            update_acao_status(id_acao, "aguardando", tentativas=tentativas_anteriores + 1)
            
    except Exception as e:
        logging.warning(f"⚠️  Erro ao verificar ação {id_acao}: {e}")
        update_acao_status(id_acao, "erro_verificacao", tentativas=tentativas_anteriores + 1, ultimo_erro=str(e))
    
    return None


def processar_resultado_acao(id_acao: int, items: List[dict], file_path: Path) -> dict:
    """
    Processa os resultados de uma ação e salva o arquivo RESUMO com sufixo _FINAL.
    Arquivo original já foi renomeado com _ORIGINAL na Fase 1.
    O arquivo RESUMO usa apenas o nome base (sem timestamp nem _ORIGINAL) + _FINAL.
    """
    data_atual = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Extrair o nome base removendo timestamp e _ORIGINAL
    base_nome = file_path.stem  # nome sem extensão
    # Exemplo: "20251031_120000_TESTE_VALIDACAO_ORIGINAL"
    
    # Remove _ORIGINAL se existir
    if base_nome.endswith('_ORIGINAL'):
        nome_sem_original = base_nome[:-9]  # Remove '_ORIGINAL' (9 caracteres)
    else:
        nome_sem_original = base_nome
    
    # Remove timestamp (YYYYMMDD_HHMMSS_) do início se existir
    # Formato: "YYYYMMDD_HHMMSS_" = 15 caracteres + 1 underscore = 16 caracteres
    partes = nome_sem_original.split('_', 3)  # Split em até 3 underscores
    if len(partes) >= 3 and partes[0].isdigit() and partes[1].isdigit():
        # Tem timestamp no formato YYYYMMDD_HHMMSS_nome
        nome_original = '_'.join(partes[2:])  # Pega tudo depois do timestamp
    else:
        nome_original = nome_sem_original
    
    # ========================================
    # ARQUIVO RESUMO (Numero + Tem Zap)
    # ========================================
    results_resumo = []
    for it in items:
        numero_raw = it.get("destinatario") or it.get("numero") or it.get("idMailingEnvio") or it.get("id")
        numero = normalize_phone_raw(numero_raw)
        tem_zap = determine_tem_zap_from_item(it)
        results_resumo.append({"Numero": numero, "Tem Zap": tem_zap})

    df_resumo = pd.DataFrame(results_resumo, columns=["Numero", "Tem Zap"])
    
    # Salvar arquivo resumo com sufixo _FINAL
    nome_arquivo_resumo = f"{data_atual}_{nome_original}.csv"
    out_path_resumo = FINAL_FOLDER / nome_arquivo_resumo
    df_resumo.to_csv(out_path_resumo, sep=";", index=False, encoding="utf-8")
    
    # ========================================
    # ESTATÍSTICAS
    # ========================================
    total_sim = len(df_resumo[df_resumo["Tem Zap"] == "SIM"])
    total_nao = len(df_resumo[df_resumo["Tem Zap"] == "NAO"])
    
    logging.info(f"💾 Arquivo RESUMO salvo: {out_path_resumo.name}")
    logging.info(f"📊 Resultados: {len(df_resumo)} total | ✅ {total_sim} com WhatsApp | ❌ {total_nao} sem WhatsApp")

    # Remover ação do banco de dados pendentes
    remove_acao_pendente(id_acao)
    
    return {
        "file": str(file_path), 
        "idAcaoEnvio": id_acao, 
        "output_resumo": str(out_path_resumo), 
        "rows": len(df_resumo),
        "whatsapp": total_sim,
        "sem_whatsapp": total_nao,
        "status": "completed"
    }

def verificar_acoes_pendentes():
    """
    FASE 2: Verifica todas as ações pendentes no banco de dados.
    Faz uma verificação rápida de cada ação (sem esperar).
    """
    acoes = load_acoes_db()
    
    if not acoes:
        return
    
    logging.info(f"🔍 Verificando {len(acoes)} ação(ões) pendente(s)...")
    
    # Verificar token antes de processar ações
    if not verificar_renovar_token():
        logging.error("❌ Falha ao obter/renovar token. Pulando verificação de ações pendentes.")
        return
    
    acoes_concluidas = 0
    acoes_aguardando = 0
    
    for id_acao_str, acao_info in list(acoes.items()):
        id_acao = int(id_acao_str)
        arquivo_nome = acao_info.get("arquivo_nome", "desconhecido")
        tentativas = acao_info.get("tentativas", 0)
        
        logging.info(f"  📋 Ação {id_acao} ({arquivo_nome}) - Tentativa #{tentativas + 1}")
        
        try:
            # Verificar resultado (apenas uma tentativa, não bloqueia)
            resultado = verificar_resultado_acao(id_acao)
            
            if resultado:
                logging.info(f"  ✅ Ação {id_acao} concluída com sucesso!")
                acoes_concluidas += 1
            else:
                # Ainda não está pronto
                logging.info(f"  ⏳ Ação {id_acao} ainda aguardando processamento")
                acoes_aguardando += 1
                
        except Exception as e:
            logging.error(f"  ❌ Erro ao verificar ação {id_acao}: {e}")
            acoes_aguardando += 1
    
    if acoes_concluidas > 0 or acoes_aguardando > 0:
        logging.info(f"📈 Resumo: {acoes_concluidas} concluídas | {acoes_aguardando} aguardando")


# ------------- loop watcher -------------
def watcher_loop():
    """
    Loop principal com processamento em duas fases:
    
    FASE 1: Incluir todos os arquivos novos na API (envio rápido)
    FASE 2: Verificar status das ações pendentes (consulta)
    
    Esta abordagem garante que todos os arquivos sejam enviados primeiro
    antes de começar a verificar resultados, otimizando o processo.
    """
    try:
        logging.info("=" * 80)
        logging.info("🚀 Iniciando monitoramento de validação WhatsApp UNO")
        logging.info(f"📁 Pasta monitorada: {WATCH_FOLDER}")
        logging.info(f"💾 Pasta de saída: {FINAL_FOLDER}")
        logging.info(f"🔄 Intervalo de verificação: {LOOP_SECONDS} segundos")
        logging.info("=" * 80)
        
        # Fazer login inicial
        if not verificar_renovar_token():
            logging.error("❌ Falha no login inicial. Verifique as credenciais.")
            return
        
        while True:
            limpar_tela()
            logging.info("=" * 80)
            logging.info(f"⏰ Verificação em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info("=" * 80)
            
            # ========================================
            # FASE 1: INCLUIR NOVOS ARQUIVOS
            # ========================================
            logging.info("\n📤 FASE 1: Incluindo novos arquivos para validação")
            logging.info("-" * 80)
            
            # Procurar novos arquivos CSV (que não foram enviados ainda)
            all_csvs = sorted(glob.glob(os.path.join(str(WATCH_FOLDER), "*.csv")))
            csvs_para_enviar = []
            
            for c_str in all_csvs:
                c = Path(c_str)
                # Apenas arquivos diretamente na pasta WATCH_FOLDER
                if c.parent != WATCH_FOLDER:
                    continue
                
                # Ignorar arquivos já enviados ou processados
                marker_enviado = c.with_name(c.name + ".enviado")
                marker_processed = c.with_name(c.name + ".processed")
                
                if marker_enviado.exists() or marker_processed.exists():
                    continue
                    
                csvs_para_enviar.append(c)

            if not csvs_para_enviar:
                logging.info("✓ Nenhum arquivo novo para enviar")
            else:
                logging.info(f"📋 Encontrados {len(csvs_para_enviar)} arquivo(s) para enviar")
                
                for f in csvs_para_enviar:
                    try:
                        logging.info(f"\n  → {f.name}")
                        incluir_arquivo_para_validacao(f)
                    except Exception as e:
                        logging.exception(f"  ❌ Erro ao incluir {f.name}: {e}")
                
                logging.info(f"\n✅ Fase 1 concluída: {len(csvs_para_enviar)} arquivo(s) enviado(s)")
            
            # ========================================
            # FASE 2: VERIFICAR AÇÕES PENDENTES
            # ========================================
            logging.info("🔍 FASE 2: Verificando status das ações pendentes")
            logging.info("-" * 80)
            
            verificar_acoes_pendentes()
            
            # ========================================
            # ESTATÍSTICAS
            # ========================================
            logging.info("\n" + "=" * 80)
            acoes_pendentes = load_acoes_db()
            
            if acoes_pendentes:
                logging.info(f"📊 Ações pendentes: {len(acoes_pendentes)}")
                # Mostrar detalhes das ações pendentes
                for id_acao, info in list(acoes_pendentes.items())[:5]:  # Mostrar até 5
                    tentativas = info.get("tentativas", 0)
                    arquivo = info.get("arquivo_nome", "?")
                    logging.info(f"   • Ação {id_acao}: {arquivo} ({tentativas} verificações)")
                
                if len(acoes_pendentes) > 5:
                    logging.info(f"   ... e mais {len(acoes_pendentes) - 5} ações")
            else:
                logging.info("✓ Nenhuma ação pendente")
            
            logging.info("=" * 80)
            logging.info(f"💤 Aguardando {LOOP_SECONDS} segundos até próxima verificação...\n")
            time.sleep(LOOP_SECONDS)
            
    except KeyboardInterrupt:
        logging.info("\n" + "=" * 80)
        logging.info("🛑 Watcher interrompido pelo usuário. Encerrando...")
        
        # Mostrar ações pendentes ao encerrar
        acoes_pendentes = load_acoes_db()
        if acoes_pendentes:
            logging.info(f"⚠️  Existem {len(acoes_pendentes)} ação(ões) pendente(s) que serão verificadas na próxima execução:")
            for id_acao, info in acoes_pendentes.items():
                logging.info(f"   • Ação {id_acao}: {info.get('arquivo_nome', '?')}")
        
        logging.info("=" * 80)
    except Exception as e:
        logging.exception(f"❌ Watcher falhou: {e}")


if __name__ == "__main__":
    watcher_loop()
