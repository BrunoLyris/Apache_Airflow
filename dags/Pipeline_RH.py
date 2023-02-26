####Este é um script python para airflow que contempla as seguintes etapas:
#1º extração de dados de um OLTP mysql;
#2º carregamento desses dados para um diretório de stage;
#3° Leitura dos dados do diretório stage e tratamento/transformação
#4º carregamento dos dados transformados em 02 arquivos no diretório de stage
#5° carregamento dos dois arquivos em um OLAP postgresql.
#6º realizar limpeza dos arquivos .csv do diretorio de stage.
#------------------------------------------------------------------------------------------------------------
#Foram usados para a execução desta DAG
#	apache-airflow==2.3.2 |Python 3.9.5 | psycopg2==2.9.1 | SQLAlchemy==1.4.0 | pandas-1.4.4 |
#------------------------------------------------------------------------------------------------------------
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd 
import sqlalchemy


with DAG(
        "Pipeline_RH",
        description="Pipeline para extração oltp para tranformaçao e carga em olap",
        start_date=days_ago(1),
        schedule_interval = '@daily',
    ) as dag:

        def extrair_dados():
            #conectando a base de dados OLTP (mysql).
            mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://bruno:Teste@1234@127.0.0.1:3306/empresa')

            #selecionando os dados da tabela rh
            dados_rh=pd.read_sql_query(r"""SELECT * FROM rh""",mysql_oltp
            )

            #exportando os dados para pasta stage.
            dados_rh.to_csv('/home/lyris/dados/stage/base_rh.csv', index=False)

        def tranformar_dados():
            #carrregando os dados
            dados_rh=pd.read_csv('/home/lyris/dados/stage/base_rh.csv')
            #transformando os dados
            dados_rh_desligados= dados_rh.dropna(subset='Data_Desligamento')
            dados_rh_ativos=dados_rh[dados_rh['Data_Desligamento'].isna()]
            dados_rh_ativos.drop(columns=['Data_Desligamento','Tipo_Desligamento'],inplace=True)
            #exportando dados para stage
            dados_rh_desligados.to_csv('/home/lyris/dados/stage/base_rh_desl.csv', index=False)
            dados_rh_ativos.to_csv('/home/lyris/dados/stage/base_rh_ativ.csv', index=False)

        def carregar_colab_ativos():
            #conectando ao olap pg
            postgresql_olap = sqlalchemy.create_engine('postgresql+psycopg2://postgres:1234@127.0.0.1:5432/database_rh')
            #selecionando os dados na stage
            dados_rh_ativos=pd.read_csv('/home/lyris/dados/stage/base_rh_ativ.csv')
            #carregando para o banco pg
            dados_rh_ativos.to_sql("colaboradores_ativos",postgresql_olap,if_exists="replace",index=False)

        def carregar_colab_desligados():
            postgresql_olap = sqlalchemy.create_engine('postgresql+psycopg2://postgres:1234@127.0.0.1:5432/database_rh')
            #selecionando os dados na stage
            dados_rh_desligados=pd.read_csv('/home/lyris/dados/stage/base_rh_desl.csv')
            #carregando para o banco pg
            dados_rh_desligados.to_sql("colaboradores_desligados",postgresql_olap,if_exists="replace",index=False)

        T_extrair = PythonOperator(
            task_id="extracao_oltp",
            python_callable=extrair_dados
        )
        T_transformar = PythonOperator(
            task_id="transformar_dados",
            python_callable=tranformar_dados
        )
        T_carregar_ativ = PythonOperator(
            task_id="carregar_colab_ativos",
            python_callable=carregar_colab_ativos
        )
        T_carregar_desl = PythonOperator(
            task_id="carregar_colab_desligados",
            python_callable=carregar_colab_desligados
        )
        T_limpar= BashOperator(
            task_id="limpar",
            bash_command="rm -f /home/lyris/dados/stage/*.csv"
        )

        T_extrair>>T_transformar>>T_carregar_ativ>>T_limpar
        T_transformar>>T_carregar_desl>>T_limpar

