####Este é um script python para airflow que contempla as seguintes etapas:
#1º verfifica a disponibilização de um diretório com arquivos json por sensor a cada 30segundos executado toda sexta feira as 07:00;
#2º quando a pasta é disponibilizada, os arquivos sao copiados para outra pasta de stage(pasta=arquivos);
#3° os dados sao tranformados e unificados em um unico arquivo .csv no diretorio arquivo
#4º a pasta stage é então excluída para que seja gerada novamente na proxima sexta feira.
#5° os arquivos json da pasta "arquivos" sao também excluídos, ficando apenas o .csv gerado.
#------------------------------------------------------------------------------------------------------------
#Foram usados para a execução desta DAG
#	apache-airflow==2.3.2 |Python 3.9.5 | psycopg2==2.9.1 | SQLAlchemy==1.4.0 | pandas-1.4.4 |
#------------------------------------------------------------------------------------------------------------
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
import pandas as pd
import glob
import json

with DAG(
        "Pipeline_json_p_csv",
        description="Pipeline para união de varios Json em uma pasta para um arquivos CSV",
        start_date=days_ago(1),
        schedule_interval = '0 7 * * 5', #executa toda sexta feira as 07:00 da manha
    ) as dag:

    def tratar_dados_json_csv():
        #criando um Dataframe para armazenamento dos dados
        dados=pd.DataFrame()
        #fazendo a uniao dos .json
        for arquivo in glob.glob('/home/lyris/dados/arquivos/*.json'):
            dados_json=pd.read_json(arquivo)  

            dados=pd.concat([dados, dados_json])
        #criando csv 
        dados.to_csv('/home/lyris/dados/arquivos/dados.csv',index=False)

    checar_diretorio=FileSensor(
        task_id = "Check_diretorio",
        poke_interval = 30,
        filepath = "/home/lyris/dados/stage"
    )

    mover_arquivos=BashOperator(
        task_id="Mover_arquivos_Json",
        bash_command="mv /home/lyris/dados/stage/*.json /home/lyris/dados/arquivos/ "
    )

    transformar_arquivos=PythonOperator(
        task_id="Tranformando_arquivos",
        python_callable=tratar_dados_json_csv
    )
    limpar_json_stage=BashOperator(
        task_id="limpar_stage",
        bash_command="rm -r /home/lyris/dados/stage"
    )
    limpar_json_arquivo=BashOperator(
        task_id="limpar_arquivo",
        bash_command="rm -f /home/lyris/dados/arquivos/*.json"
    )

    checar_diretorio >> mover_arquivos >> transformar_arquivos >> limpar_json_stage
    transformar_arquivos >> limpar_json_arquivo