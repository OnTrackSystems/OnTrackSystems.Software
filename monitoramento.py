import psutil as ps
import pandas as pd
import time
from datetime import datetime
import boto3
import os

CPU_POR_ONIBUS = 1.5      
RAM_MB_POR_ONIBUS = 50    
INTERVALO_COLETA_SEGUNDOS = 5 
INTERVALO_UPLOAD_SEGUNDOS = 30 

def contar_onibus_na_garagem(caminho_arquivo=".onibusAtuais"): 
    try:
        with open(caminho_arquivo, 'r') as f:
            num_onibus = sum(1 for line in f if line.strip())
        return num_onibus
    except FileNotFoundError:
        return 0
    except Exception as e:
        print(f"Erro ao ler o arquivo .onibusAtuais: {e}")
        return 0

def obter_uso():
    global dados

    cpu_real_percent = ps.cpu_percent(interval=1)
    ram_real = ps.virtual_memory()
    disco = ps.disk_usage('/')
    rede = ps.net_io_counters(pernic=False, nowrap=True)
    usuario = ps.users()
    num_processos = len(list(ps.process_iter()))
    user = usuario[0].name if usuario else "Desconhecido"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
    dados["Onibus_Garagem"].append(num_onibus)

def salvar_csv():
    global dados

    df = pd.DataFrame(dados)
    df.to_csv("coletaGeralOTS.csv", encoding="utf-8", index=False)

def subirCSVS3():
    client = boto3.client('s3')
    bucket = 'raw-ontrack' 
    arquivo = 'coletaGeralOTS.csv'

    try:
        print(f"\n--- Subindo '{arquivo}' para o bucket S3 '{bucket}' ---")
        client.upload_file(arquivo, bucket, arquivo)
        print("--- Upload para o S3 concluído com sucesso! ---")
    except FileNotFoundError:
        print(f"Arquivo {arquivo} não encontrado para upload.")
    except Exception as e:
        print(f"--- Falha ao subir o arquivo para o S3: {e} ---")

def monitoramento():
    tempo_desde_ultimo_upload = 0
    try:
        while True:
            obter_uso()
            salvar_csv()

            tempo_desde_ultimo_upload += INTERVALO_COLETA_SEGUNDOS

            if tempo_desde_ultimo_upload >= INTERVALO_UPLOAD_SEGUNDOS:
                subirCSVS3()
                tempo_desde_ultimo_upload = 0
                
            time.sleep(INTERVALO_COLETA_SEGUNDOS) 

    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuário.")

        resposta = input("Deseja fazer um último upload para a AWS com os dados coletados? (s/n): ").strip().lower()
        if resposta == 's':
            print("\nRealizando último upload...")
            salvar_csv() 
            subirCSVS3()


monitoramento()
print("\nPrograma finalizado.")