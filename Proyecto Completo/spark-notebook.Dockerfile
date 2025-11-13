FROM jupyter/pyspark-notebook:latest

USER root
RUN pip install psycopg2-binary
# Instala el driver JDBC de PostgreSQL
ENV POSTGRES_JDBC_VERSION=42.7.1
RUN wget -O /usr/local/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
	https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar
USER jovyan
