import psutil as ps
import pandas as pd
import time
from datetime import datetime
import boto3
import os
import requests
from requests.auth import HTTPBasicAuth
import json
from dotenv import load_dotenv

load_dotenv('.env.dev')

# --- CONFIGURAÇÕES JIRA ---
JIRA_DOMAIN = 'https://ontracksys.atlassian.net'
# Verifique se a Key do projeto é 'CHM' ou 'ChamadosOntrack' no seu Jira
PROJECT_KEY = "CHM" 
JIRA_CORE_URL = f"{JIRA_DOMAIN}/rest/api/3/issue"

JIRA_EMAIL = os.getenv('JIRA_EMAIL')
JIRA_TOKEN = os.getenv('JIRA_API_TOKEN')

print(f"DEBUG JIRA_EMAIL: {'Carregado' if JIRA_EMAIL else 'NAO CARREGADO (None)'}")
print(f"DEBUG JIRA_TOKEN: {'Carregado' if JIRA_TOKEN else 'NAO CARREGADO (None)'}")

# --- CONFIGURAÇÕES DE MONITORAMENTO ---
CPU_POR_ONIBUS = 1.5      
RAM_MB_POR_ONIBUS = 50    
INTERVALO_COLETA_SEGUNDOS = 5 
INTERVALO_UPLOAD_SEGUNDOS = 30
COOLDOWN_SEGUNDOS = 300 # 5 minutos

# --- VARIÁVEIS DE DADOS ---
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

# Variáveis globais de garagem
nome_garagem = ""
id_garagem = ""

# --- FUNÇÕES AUXILIARES ---
def bytes_para_mb(bytes_value):
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

def get_id_garagem(caminho_arquivo=".uuid"):
    global nome_garagem, id_garagem
    try:
        with open(caminho_arquivo, 'r') as f:
            parametros = f.readline().split(',')
            if len(parametros) >= 5:
                id_garagem = parametros[0].strip()   
                nome_garagem = parametros[1].strip()                 
                return parametros[4].strip()
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

def salvar_csv_unico():
    global dados
    if not dados["timestamp"]:
        return
    
    ultima_linha = {col: dados[col][-1] for col in dados}

    with open("coletaUnicaOTS.json", "w", encoding="utf-8") as f:
        json.dump(ultima_linha, f, ensure_ascii=False, indent=4)


def subirCSVS3():
    idGaragem = get_id_garagem()
    anoAtual = datetime.now().strftime('%Y')
    mesAtual = datetime.now().strftime('%m')
    diaAtual = datetime.now().strftime('%d')
    horaAtual = datetime.now().strftime('%H')
    minutoAtual = datetime.now().strftime('%M')
    segundoAtual = datetime.now().strftime('%S')

    arquivo = 'coletaGeralOTS.csv'
    client = boto3.client('s3')
    bucket = 's3-raw-ontracksystems' 
    caminhos3 = 'idGaragem={}/ano={}/mes={}/dia={}/hora={}/coleta_{}{}{}.csv'.format(idGaragem, anoAtual, mesAtual, diaAtual, horaAtual, horaAtual, minutoAtual, segundoAtual)

    try:
        print(f"\n--- Subindo '{arquivo}' para o bucket S3 '{bucket}' ---")
        client.upload_file(arquivo, bucket, caminhos3)
        print("--- Upload para o S3 concluido com sucesso! ---")
    except FileNotFoundError:
        print(f"Arquivo {arquivo} nao encontrado para upload.")
    except Exception as e:
        print(f"--- Falha ao subir o arquivo para o S3: {e} ---")

def subir_csv_unico_s3():
    idGaragem = get_id_garagem()

    client = boto3.client('s3')
    bucket = 's3-raw-ontracksystems'

    caminho_s3 = f"idGaragem={idGaragem}/snapshot/coletaUnicaOTS.json"

    try:
        print("\n--- Enviando coleta única (JSON) para o S3 ---")
        client.upload_file("coletaUnicaOTS.json", bucket, caminho_s3)
        print("--- Upload do arquivo único concluído! ---")
    except Exception as e:
        print(f"Erro ao enviar arquivo único: {e}")


def monitoramento():
    global dados
    tempo_desde_ultimo_upload = 0
    get_id_garagem() 
    
    print("Iniciando monitoramento (Ciclo 2025/2)...")
    try:
        while True:
            obter_uso()
            salvar_csv()
            salvar_csv_unico()
            verificar_alertas()
            
            if dados['timestamp']:
                print(f"[{dados['timestamp'][-1]}] CPU: {dados['CPU'][-1]:.2f}%, RAM: {dados['RAM_Percent'][-1]:.2f}%, Onibus: {dados['Onibus_Garagem'][-1]}")

            tempo_desde_ultimo_upload += INTERVALO_COLETA_SEGUNDOS
            if tempo_desde_ultimo_upload >= INTERVALO_UPLOAD_SEGUNDOS:
                subirCSVS3()
                subir_csv_unico_s3()
                print("--- Lote enviado. Limpando dados para o proximo ciclo. ---")
                dados = {key: [] for key in dados}
                tempo_desde_ultimo_upload = 0
                
            time.sleep(INTERVALO_COLETA_SEGUNDOS) 

    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuario.")
        if any(dados.values()) and len(dados['timestamp']) > 0:
            try:
                resposta = input("Deseja fazer um ultimo upload para a AWS? (s/n): ").strip().lower()
                if resposta == 's':
                    print("\nRealizando ultimo upload...")
                    salvar_csv() 
                    subirCSVS3()
            except EOFError:
                pass

# --- ESTADO DE ALERTAS ---
ultimo_alerta_critico = { "CPU": 0, "RAM": 0, "Disco": 0 }
ultimo_alerta_medio = { "CPU": 0, "RAM": 0, "Disco": 0 }

def abrir_chamado_jira(componente, valor_atual, limite, nivel):
    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    if nivel == "CRITICO":
        tag = "[ALERTA CRITICO]"
        jira_priority = "Highest" 
    elif nivel == "MEDIO":
        tag = "[ALERTA MEDIO]"
        jira_priority = "Medium"
    else:
        tag = "[ALERTA]"
        jira_priority = "Medium"

    payload = {
        "fields": {
            "project": {"key": PROJECT_KEY},
            "summary": f"{tag} {componente} atingiu {valor_atual:.2f}% de uso - Garagem: {nome_garagem}",
            "issuetype": {"name": "Task"}, 
            "priority": {"name": jira_priority},
            "labels": ["Monitoramento", "2025-2"],
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {
                                "type": "text",
                                "text": f"Monitoramento 2025/2 - Anomalia detectada (Nivel {nivel})."
                            },
                            {
                                "type": "text",
                                "text": f"Componente: {componente} Valor Atual: {valor_atual:.2f}% Limite: {limite}% ID: {id_garagem}",
                                "marks": [{"type": "strong"}]
                            }
                        ]
                    }
                ]
            }
        }
    }

    try:
        print(f"Tentando abrir chamado JIRA ({nivel})...")
        response = requests.post(JIRA_CORE_URL, json=payload, headers=headers, auth=auth)

        if response.status_code == 201:
            ticket_key = response.json().get('key')
            print(f"[SUCESSO] Chamado JIRA ({nivel}) criado! Chave: {ticket_key}")
            return True
        else:
            print(f"[ERRO] Falha ao abrir chamado: {response.status_code}")
            print("Detalhes do erro JIRA:", response.text)
            return False

    except Exception as e:
        print(f"[ERRO] Conexao com JIRA falhou: {e}")
        return False

def verificar_alertas():
    if not dados['timestamp']:
        return

    # Limites
    LIMITES_CRITICOS = { "CPU": 90.0, "RAM": 90.0, "Disco": 95.0 }
    LIMITES_MEDIOS = { "CPU": 70.0, "RAM": 75.0, "Disco": 85.0 }

    cpu_val = dados['CPU'][-1]
    ram_val = dados['RAM_Percent'][-1]
    disco_percent = ps.disk_usage('/').percent
    agora = time.time()
    
    # Rastreia quem alertou crítico neste ciclo para não alertar médio depois
    componentes_criticos_neste_ciclo = set()

    # --- 1. CHECAGEM CRÍTICA ---
    for componente, valor_atual in [("CPU", cpu_val), ("RAM", ram_val), ("Disco", disco_percent)]:
        limite = LIMITES_CRITICOS[componente]
        
        if valor_atual > limite:
            # Adiciona ao set para evitar duplicidade de alerta (Crítico + Médio)
            componentes_criticos_neste_ciclo.add(componente)
            
            if (agora - ultimo_alerta_critico[componente]) > COOLDOWN_SEGUNDOS:
                if abrir_chamado_jira(componente, valor_atual, limite, "CRITICO"):
                    ultimo_alerta_critico[componente] = agora
            
            # REMOVIDO O RETURN QUE CAUSAVA O BUG

    # --- 2. CHECAGEM MÉDIA ---
    for componente, valor_atual in [("CPU", cpu_val), ("RAM", ram_val), ("Disco", disco_percent)]:
        
        # Se já deu crítico agora, pula o médio
        if componente in componentes_criticos_neste_ciclo:
            continue

        limite = LIMITES_MEDIOS[componente]
        
        if valor_atual > limite:
            if (agora - ultimo_alerta_medio[componente]) > COOLDOWN_SEGUNDOS:
                if abrir_chamado_jira(componente, valor_atual, limite, "MEDIO"):
                    ultimo_alerta_medio[componente] = agora

if __name__ == "__main__":
    monitoramento()
    print("\nPrograma finalizado.")