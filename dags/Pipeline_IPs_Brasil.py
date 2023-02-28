
####Este é um script python para airflow que contempla as seguintes etapas:
#1º conecta ao site e de IPs validos na américa do sul.
#2º cria um dataframe com os dados de disponíveis na URL.
#3° limpa e transforma os dados.
#4º salva o dataframe em um repositório.
#5° envia um email de notificação de IPs Atualizados, com o arquivo em anexo.
#------------------------------------------------------------------------------------------------------------
#Foram usados para a execução desta DAG
#	apache-airflow==2.3.2 |Python 3.9.5 | psycopg2==2.9.1 | SQLAlchemy==1.4.0 | pandas-1.4.4 |
#------------------------------------------------------------------------------------------------------------

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
import pandas as pd

with DAG(
    'IPs_Brasil',
     start_date=days_ago(1),
     schedule_interval = '0 0 2 * *', #executa todo 2º dia de cada mês
    ) as dag:

    def carregar_dados():
        #coletando os dados:
        url='https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest'
        
        #transformando os dados:
        colunas_nomes = ['origem','País','tipo_ip','IP','tam_subnet','Dt_status','status','extesões']
        dados_ip=pd.read_csv(url,skiprows=4,names=colunas_nomes,sep='|')
        
        dados_ip['Dt_status']=pd.to_datetime(dados_ip['Dt_status'],format=('%Y%m%d'))
        
        dados_ip=dados_ip[dados_ip['País']=='BR']
        dados_ip=dados_ip[dados_ip['tipo_ip']=='ipv4']

        dados_ip.to_csv('/home/lyris/dados/arquivos/ips_brasil.csv',index=False)

    Coletar_ips=PythonOperator(
        task_id="coletar_IPs",
        python_callable=carregar_dados
    )

    Envio_email=EmailOperator(
        task_id='Notificacao_email',
        to='brunolyris@gmail.com',
        subject='IPs Atualizados',
        files=['/home/lyris/dados/arquivos/ips_brasil.csv'],
        html_content='<p> O lista de IPs válidos foi atualizada!</p>'
    )

    Coletar_ips>>Envio_email