FROM apache/airflow:2.8.4

#RUN apt-get install unzip
#RUN wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/120.0.6099.71/linux64/chromedriver-linux64.zip
#RUN unzip chromedriver-linux64.zip
#RUN cd chromedriver-linux64
#RUN sudcp chromedriver  /usr/local/bin/
#RUN  chmod +x chromedriver

#RUN wget http://dl.google.com/linux/deb/pool/main/g/google-chrome-stable/google-chrome-stable_120.0.6099.71-1_amd64.deb
#RUN  apt-get install -f ./google-chrome-stable_120.0.6099.71-1_amd64.deb

RUN  apt update
RUN  apt install pkg-config -y
RUN  apt install python3-dev build-essential -y
RUN  apt install libmysqlclient-dev -y
RUN  apt install libssl-dev -y
RUN  apt install libkrb5-dev -y

RUN  apt install pkg-config -y
RUN  apt install python3-dev build-essential -y
RUN  apt install mysql-server -y
RUN  apt install libmysqlclient-dev -y
RUN  apt install libssl-dev -y
RUN  apt install libkrb5-dev -y
RUN  apt install python3-pip -y

RUN  pip install mysql-connector-python 
RUN  pip install mysqlclient 

RUN  pip install cx_Oracle 
RUN  pip install pandas 
RUN  pip install jupyterlab 
RUN  pip install openpyxl 
RUN  pip install xlsxwriter 
RUN  pip install xlwings 
RUN  pip install Office365-REST-Python-Client 
RUN  pip install office365 
RUN  pip install pysharepoint 
RUN  pip install xlrd
RUN  pip install SQLAlchemy 
RUN  pip install selenium
RUN  pip install apache-airflow-providers-mysql
RUN  pip install apache-airflow-providers-oracle
RUN  pip install apache-airflow-providers-postgres
RUN  pip install apache-airflow-providers-apache-spark
RUN  pip install apache-airflow-providers-databricks
