FROM apache/airflow:2.7.3

USER root

# Instala Java 17
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

# Configura JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Instala dependências Python com versões compatíveis
RUN pip install \
    apache-airflow-providers-papermill==2.1.0 \
    pyspark==3.5.0 \
    jupyterlab


