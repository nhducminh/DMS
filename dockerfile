FROM apache/airflow:2.10.2

USER root
RUN apt-get update
RUN apt-get install wget
RUN apt-get install unzip
RUN wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/120.0.6099.71/linux64/chromedriver-linux64.zip
RUN unzip chromedriver-linux64.zip
RUN chmod +x chromedriver-linux64/chromedriver
RUN rm chromedriver-linux64.zip

RUN wget http://dl.google.com/linux/deb/pool/main/g/google-chrome-stable/google-chrome-stable_120.0.6099.71-1_amd64.deb
RUN apt-get install -f ./google-chrome-stable_120.0.6099.71-1_amd64.deb -y
RUN rm google-chrome-stable_120.0.6099.71-1_amd64.deb


RUN apt install pkg-config python3-dev build-essential python3-pip libmysqlclient-dev libssl-dev libkrb5-dev -y

COPY requirement.txt .
COPY ./.env .
USER airflow
RUN pip install -r requirement.txt
