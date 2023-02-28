# Apache_Airflow
Neste diretório encontram-se os scripts/DAGs de transformação/controle de dados por mim desenvolvidos em plataforma apache-airflow.

# Contents
>> - Pipeline_RH 
    Resumo: Extração de dados OLTP(Mysql), transformação, carga em um OLAP(Postgres) e limpeza do diretório de stage.

>> - Pipeline_json_to_csv 
    Resumo: Checagem de carga de um diretorio, tranformação dos arquivos json, carga em um único arquivo CSV e limpeza das stages.

>> - Pipeline_IPs_Brasil 
    Resumo: Coleta os IPs validos na america do sul atraves de url, tranformação e limpeza dos dados, carrega em um diretorio como CSV e envia um email de notificação               com o arquivo em anexo.

