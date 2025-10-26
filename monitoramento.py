import psutil as ps
import pandas as pd
import time
from datetime import datetime
import boto3

dados = {
    "timestamp": [],
    "usuario": [],
    "CPU": [],
    # descomente a linha abaixo se estiver no Linux
    # "tempoI/O": [],
    "RAM": [],
    "RAM_Percent": [],
    "Disco": [],
    "PacotesEnv": [],
    "PacotesRec": [],
    "Num_processos": []
}


def obter_uso():
    cpu = ps.cpu_times(percpu=False)
    cpuPercent = ps.cpu_percent(interval=1)
    RAM = ps.virtual_memory()
    RAM_Percent = ps.virtual_memory().percent
    disco = ps.disk_usage('/')
    rede = ps.net_io_counters(pernic=False, nowrap=True)
    usuario = ps.users()
    num_processos = len(list(ps.process_iter()))
    user = usuario[0].name if usuario else "Desconhecido"
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    dados["timestamp"].append(timestamp)
    dados["usuario"].append(user)
    dados["CPU"].append(cpuPercent)
    dados["RAM"].append(round(RAM.used / 1024 ** 3))
    dados["RAM_Percent"].append(RAM_Percent)
    dados["Disco"].append(round(disco.used / 1024 ** 3))
    dados["PacotesEnv"].append(rede.packets_sent)
    dados["PacotesRec"].append(rede.packets_recv)
    dados["Num_processos"].append(num_processos)
    # Descomente a linha abaixo se estiver em um Linux
    # dados["tempoI/O"].append(cpu.iowait)


def salvar_csv():
    df = pd.DataFrame(dados)
    df.to_csv("coletaGeralOTS.csv", encoding="utf-8", index=False)


def monitoramento():
    try:
        while True:
            obter_uso()

            # Exibição no terminal
            print(
                f"\nData/Hora: {dados['timestamp'][-1]}"
                f"\nUsuário: {dados['usuario'][-1]}"
                f"\nUso da CPU: {dados['CPU'][-1]}%"
                # Descomente a linha abaixo se estiver em Linux
                # f"\nTempo de I/O: {dados['tempoI/O'][-1]}s"
                f"\nRAM: {dados['RAM'][-1]} Gb"
                f"\nPercentual de uso de RAM: {dados['RAM_Percent'][-1]}%"
                f"\nDisco usado: {dados['Disco'][-1]} Gb"
                f"\nPacotes Enviados: {dados['PacotesEnv'][-1]}"
                f"\nPacotes Recebidos: {dados['PacotesRec'][-1]}"
                f"\nQuantidade de processos: {dados['Num_processos'][-1]}\n"
            )

            salvar_csv()
            time.sleep(1)

    except KeyboardInterrupt:
        resposta = input("\nVocê deseja parar o monitoramento? (s/n): ").strip().lower()
        if resposta == 's':
            print("\nMonitoramento finalizado.")
        elif resposta == 'n':
            print("\nO monitoramento continuará.")
            monitoramento()
        else:
            print("\nOpção inválida. Monitoramento continuará.")
            monitoramento()


def subirCSVS3():
    # IMPORTANTE!
    # Rodar um "aws configure" e atualizar as informações da sessão da aws, caso contrário, o boto3 não vai conseguir acessar a aws
    
    client = boto3.client('s3')
    #substituir pelo nome do bucket raw
    bucket = 'raw-ontrack' 
    arquivo = 'coletaGeralOTS.csv'

    client.upload_file(arquivo, bucket, arquivo)
    print("Arquivo adicionado ao bucket com sucesso!")


# Execução principal
monitoramento()
subirCSVS3()
