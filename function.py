import boto3
import json

def start_glue_job(job_name, job_arguments):
    """
    Função para iniciar um job no AWS Glue.
    
    Args:
    - job_name: O nome do job Glue que você quer iniciar.
    - job_arguments: Um dicionário contendo os argumentos que serão passados para o job.
    """
    glue_client = boto3.client('glue')
    response = glue_client.start_job_run(JobName=job_name, Arguments=job_arguments)
    return response

def process_kafka_event(event):
    """
    Função para processar o evento recebido do Kafka.
    
    Args:
    - event: O evento recebido do Kafka.
    """
    # Decodifica o payload do evento
    payload = json.loads(event)
    
    # Extrai os dados relevantes do payload
    nome_tabela = payload['nome_tabela']
    sigla = payload['sigla']
    data_hora_ingestao = payload['data_hora_ingestao']
    timestamp = payload['timestamp']
    
    # Construa os argumentos do job Glue
    job_arguments = {
        'nome_tabela': nome_tabela,
        'sigla': sigla,
        'data_hora_ingestao': data_hora_ingestao,
        'timestamp': timestamp
        # Adicione outros argumentos conforme necessário
    }
    
    # Nome do job Glue que você quer iniciar
    job_name = 'seu_job_glue'
    
    # Inicia o job Glue
    start_glue_job(job_name, job_arguments)

def lambda_handler(event, context):
    """
    Função de manipulador da Lambda.
    
    Args:
    - event: O evento de entrada recebido pela Lambda (nesse caso, o evento do Kafka).
    - context: O contexto de execução da Lambda.
    """
    # Itera sobre os registros recebidos do Kafka
    for record in event['Records']:
        # Obtém o payload do registro
        payload = record['body']
        
        # Processa o evento do Kafka
        process_kafka_event(payload)
