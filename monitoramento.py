import psutil as ps
import pandas as pd
import time
from datetime import datetime
import boto3
import requests
from requests.auth import HTTPBasicAuth
import json
import os

# --- CONFIGURAÇÕES DO AMBIENTE (GITHUB ACTIONS / SISTEMA) ---
JIRA_EMAIL = os.environ.get('EMAIL_JIRA')
JIRA_TOKEN = os.environ.get('TOKEN_JIRA')


# Debug para confirmar se o GitHub injetou corretamente
print(f"DEBUG JIRA_EMAIL: {'Carregado' if JIRA_EMAIL else 'NÃO CARREGADO (None)'}")
print(f"DEBUG JIRA_TOKEN: {'Carregado' if JIRA_TOKEN else 'NÃO CARREGADO (None)'}")

JIRA_DOMAIN = 'https://ontracksys.atlassian.net'
PROJECT_KEY = "CHAM"
JSM_URL = f"{JIRA_DOMAIN}/rest/servicedeskapi/request"
SERVICEDESK_ID = "2"
REQUEST_TYPE_ID = "10050"

CPU_POR_ONIBUS = 1.5      
RAM_MB_POR_ONIBUS = 50    
INTERVALO_COLETA_SEGUNDOS = 5 
INTERVALO_UPLOAD_SEGUNDOS = 30

dados = {
    "timestamp": [], "usuario": [], "CPU": [], "RAM": [], "RAM_Percent": [],
    "Disco": [], "PacotesEnv": [], "PacotesRec": [], "Num_processos": [],
    "MB_Enviados_Seg": [], "MB_Recebidos_Seg": [], 
    "MB_Total_Enviados": [], "MB_Total_Recebidos": [],
    "Onibus_Garagem": []
}

stats_iniciais = ps.net_io_counters(pernic=False, nowrap=True)
bytes_sent_init = stats_iniciais.bytes_sent
bytes_recv_init = stats_iniciais.bytes_recv


def bytes_para_mb(bytes_value):
    """Converte bytes para Megabytes"""
    return bytes_value / (1024 * 1024)

def contar_onibus_na_garagem(caminho_arquivo=".onibusAtuais"): 
    try:
        with open(caminho_arquivo, 'r') as f:
            num_onibus = sum(1 for line in f if line.strip())
        return num_onibus
    except FileNotFoundError:
        return 0
    except Exception as e:
        print(f"Erro ao ler o arquivo {caminho_arquivo}: {e}")
        return 0

# Variáveis globais de garagem
nome_garagem = ""
id_garagem = ""

def get_id_garagem(caminho_arquivo=".uuid"):
    global nome_garagem, id_garagem
    try:
        with open(caminho_arquivo, 'r') as f:
            parametros = f.readline().split(',')
            if len(parametros) >= 5:
                id_garagem = parametros[0].strip()   
                nome_garagem = parametros[1].strip()                 
                return parametros[4].strip() # Retorna índice 4 para o S3
            return "id_desconhecido"
    except FileNotFoundError:
        return "garagem_padrao"

def obter_uso():
    global dados, bytes_sent_init, bytes_recv_init

    cpu_real_percent = ps.cpu_percent(interval=1)
    ram_real = ps.virtual_memory()
    disco = ps.disk_usage('/')
    rede = ps.net_io_counters(pernic=False, nowrap=True)
    usuario = ps.users()
    num_processos = len(list(ps.process_iter()))
    user = usuario[0].name if usuario else "Desconhecido"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    bytes_sent_atual = rede.bytes_sent
    bytes_recv_atual = rede.bytes_recv

    delta_sent = bytes_sent_atual - bytes_sent_init
    delta_recv = bytes_recv_atual - bytes_recv_init

    mb_sent_seg = bytes_para_mb(delta_sent)
    mb_recv_seg = bytes_para_mb(delta_recv)

    mb_total_env = bytes_para_mb(bytes_sent_atual - stats_iniciais.bytes_sent)
    mb_total_rec = bytes_para_mb(bytes_recv_atual - stats_iniciais.bytes_recv)

    bytes_sent_init = bytes_sent_atual
    bytes_recv_init = bytes_recv_atual

    num_onibus = contar_onibus_na_garagem()

    carga_cpu_simulada = num_onibus * CPU_POR_ONIBUS
    carga_ram_simulada_bytes = num_onibus * RAM_MB_POR_ONIBUS * (1024 * 1024)

    cpu_final_percent = min(100.0, cpu_real_percent + carga_cpu_simulada)
    
    ram_usada_final_bytes = ram_real.used + carga_ram_simulada_bytes
    ram_usada_final_gb = round(ram_usada_final_bytes / (1024 ** 3), 2)
    ram_final_percent = min(100.0, (ram_usada_final_bytes / ram_real.total) * 100)

    dados["timestamp"].append(timestamp)
    dados["usuario"].append(user)
    dados["CPU"].append(cpu_final_percent)
    dados["RAM"].append(ram_usada_final_gb)
    dados["RAM_Percent"].append(ram_final_percent)
    dados["Disco"].append(round(disco.used / 1024 ** 3, 2))
    dados["PacotesEnv"].append(rede.packets_sent)
    dados["PacotesRec"].append(rede.packets_recv)
    dados["Num_processos"].append(num_processos)
    dados["MB_Enviados_Seg"].append(round(mb_sent_seg, 2))
    dados["MB_Recebidos_Seg"].append(round(mb_recv_seg, 2))
    dados["MB_Total_Enviados"].append(round(mb_total_env, 2))
    dados["MB_Total_Recebidos"].append(round(mb_total_rec, 2))
    dados["Onibus_Garagem"].append(num_onibus)

def salvar_csv():
    global dados
    df = pd.DataFrame(dados)
    df.to_csv("coletaGeralOTS.csv", encoding="utf-8", index=False)

def subirCSVS3():
    # Nota: get_id_garagem retorna o parametro[4] para uso no caminho S3
    idGaragemS3 = get_id_garagem() 

    anoAtual = datetime.now().strftime('%Y')
    mesAtual = datetime.now().strftime('%m')
    diaAtual = datetime.now().strftime('%d')
    horaAtual = datetime.now().strftime('%H')
    minutoAtual = datetime.now().strftime('%M')
    segundoAtual = datetime.now().strftime('%S')

    arquivo = 'coletaGeralOTS.csv'
    
    # Boto3 pega automaticamente AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY do ambiente
    client = boto3.client('s3')
    
    # Se quiser pegar o bucket do ambiente também:
    bucket = os.getenv('S3_BUCKET_NAME') or 's3-raw-ontracksystems'
    
    caminhos3 = 'idGaragem={}/ano={}/mes={}/dia={}/hora={}/coleta_{}{}{}.csv'.format(idGaragemS3, anoAtual, mesAtual, diaAtual, horaAtual, horaAtual, minutoAtual, segundoAtual)

    try:
        print(f"\n--- Subindo '{arquivo}' para o bucket S3 '{bucket}' ---")
        client.upload_file(arquivo, bucket, caminhos3)
        print("--- Upload para o S3 concluído com sucesso! ---")
    except FileNotFoundError:
        print(f"Arquivo {arquivo} não encontrado para upload.")
    except Exception as e:
        print(f"--- Falha ao subir o arquivo para o S3: {e} ---")
        
def monitoramento():
    global dados
    tempo_desde_ultimo_upload = 0
    
    get_id_garagem() 
    
    print("Iniciando monitoramento...")
    try:
        while True:
            obter_uso()
            salvar_csv()
            
            verificar_alertas()
            
            if dados['timestamp']:
                print(f"[{dados['timestamp'][-1]}] CPU: {dados['CPU'][-1]:.2f}%, RAM: {dados['RAM_Percent'][-1]:.2f}%, Ônibus: {dados['Onibus_Garagem'][-1]}")

            tempo_desde_ultimo_upload += INTERVALO_COLETA_SEGUNDOS

            if tempo_desde_ultimo_upload >= INTERVALO_UPLOAD_SEGUNDOS:
                subirCSVS3()
                
                print("--- Lote enviado. Limpando dados para o próximo ciclo. ---")
                dados = {key: [] for key in dados}
                
                tempo_desde_ultimo_upload = 0
                
            time.sleep(INTERVALO_COLETA_SEGUNDOS) 

    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuário.")

        if any(dados.values()) and len(dados['timestamp']) > 0:
            try:
                resposta = input("Deseja fazer um último upload para a AWS com os dados restantes? (s/n): ").strip().lower()
                if resposta == 's':
                    print("\nRealizando último upload...")
                    salvar_csv() 
                    subirCSVS3()
            except EOFError:
                pass


ultimo_alerta = {
    "CPU": 0,
    "RAM": 0,
    "Disco": 0
}
COOLDOWN_SEGUNDOS = 300

def abrir_solicitacao_jsm(componente, valor_atual, limite):
    """Cria uma solicitação no Jira Service Management (JSM)."""
    
    if not JIRA_EMAIL or not JIRA_TOKEN:
        print(" Erro: Credenciais do Jira não carregadas. Verifique as Secrets do GitHub.")
        return

    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    summary_tag = "[ALERTA CRÍTICO]" 

    payload = {
        "serviceDeskId": SERVICEDESK_ID,
        "requestTypeId": REQUEST_TYPE_ID,
        "requestFieldValues": {
            "summary": f"{summary_tag} {componente} atingiu {valor_atual:.2f}% de uso.",
            "description": (
                f"O monitoramento detectou uso perigoso:\n"
                f"- Componente: {componente}\n"
                f"- Valor Atual: {valor_atual:.2f}%\n"
                f"- Limite Definido: {limite}%\n"
                f"- Garagem: {nome_garagem}\n"
                f"- ID da Garagem: {id_garagem}"
            ),
        },
        
    }

    try:
        print(f"Tentando abrir solicitação JSM ({REQUEST_TYPE_ID})...")
        response = requests.post(JSM_URL, json=payload, headers=headers, auth=auth)

        if response.status_code == 201:
            ticket_key = response.json().get('issueKey')
            print(f"Solicitação JSM criada com sucesso! Chave: {ticket_key}")
        else:
            print(f"Erro ao abrir solicitação: {response.status_code}")
            print("Detalhes do erro JSM:")
            print(json.dumps(response.json(), indent=4, ensure_ascii=False))

    except Exception as e:
        print(f" Erro de conexão com Jira JSM: {e}")

def verificar_alertas():
    if not dados['timestamp']:
        return

    # --- DEFINA SEUS LIMITES AQUI ---
    LIMITES = {
        "CPU": 90.0,        # x%
        "RAM": 60.0,        # x%
        "Disco": 95.0       # x% 
    }

    
    # 1.CPU
    cpu_val = dados['CPU'][-1]
    if cpu_val > LIMITES['CPU']:
        agora = time.time()
        if (agora - ultimo_alerta['CPU']) > COOLDOWN_SEGUNDOS:
            abrir_solicitacao_jsm("CPU", cpu_val, LIMITES['CPU'])
            ultimo_alerta['CPU'] = agora

    # 2.RAM (%)
    ram_val = dados['RAM_Percent'][-1]
    if ram_val > LIMITES['RAM']:
        agora = time.time()
        if (agora - ultimo_alerta['RAM']) > COOLDOWN_SEGUNDOS:
            abrir_solicitacao_jsm("RAM", ram_val, LIMITES['RAM'])
            ultimo_alerta['RAM'] = agora

    # Disco
    disco_percent = ps.disk_usage('/').percent
    if disco_percent > LIMITES['Disco']:
        agora = time.time()
        if (agora - ultimo_alerta['Disco']) > COOLDOWN_SEGUNDOS:
            abrir_solicitacao_jsm("Disco", disco_percent, LIMITES['Disco'])
            ultimo_alerta['Disco'] = agora


if __name__ == "__main__":
    monitoramento()
    print("\nPrograma finalizado.")