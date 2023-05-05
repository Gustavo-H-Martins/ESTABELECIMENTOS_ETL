# Como instalar o Spark, Python e Pyspark localmente

Aqui estão os links, código e caminhos referenciados ao longo do texto.

## Python

https://www.python.org/downloads/
Lembre-se de marcar "set PATH" na instalação.

## Java

https://java.com/en/download/help/win...
Defina a variável de ambiente do sistema JAVA_HOME como C:\Program Files\Java{versão do jre}

## Spark

https://spark.apache.org/downloads.html
Descompacte o arquivo tar duas vezes e coloque-o em C:\Spark
Nas variáveis de ambiente do sistema, defina a variável de ambiente SPARK_HOME como C:\Spark.
Nas variáveis de ambiente do sistema, adicione um novo caminho para %SPARK_HOME%\bin.

## Hadoop

https://github.com/cdarlint/winutils
Baixe o winutils.exe.
Adicione uma pasta C:\Hadoop\bin.
Adicione winiutils a essa pasta.
Nas variáveis de ambiente do sistema, defina a variável de ambiente HADOOP_HOME como C:\Hadoop.
Nas variáveis de ambiente do sistema, adicione um novo caminho para %HADOOP_HOME%\bin.

## Confirmar o Spark

Abra o prompt de comando com privilégios de administrador.
Você deve ser capaz de executar "spark-shell" em qualquer lugar, pois já configurou as variáveis de ambiente acima e funcionará normalmente.

## Local Spark UI

http://localhost:4040/

## Pyspark

https://code.visualstudio.com/

```bash
    py -3.9 -m venv .test_env
    .test_env\scripts\activate
    pip install pyspark
    pyspark
    .test_env\scripts\deactivate
```

```PYTHON
import os
os.environ["PYSPARK_PYTHON"] = r"~\ETL_CNPJ\.venv\Lib\site-packages\pyspark-3.4.0-py3.9.egg-info"

os.environ["PYSPARK_DRIVER_PYTHON"] = r"~\ETL_CNPJ\.venv\Lib\site-packages\pyspark-3.4.0-py3.9.egg-info"

os.environ["JAVA_HOME"] = r"~\Java\jre-1.8"
```
