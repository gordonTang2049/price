FROM python:3

USER root

RUN apt-get update && apt-get upgrade -y

RUN apt-get install -y curl 

RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - 

RUN apt-get install -y nodejs

# install FreeTDS and dependencies
RUN apt-get update \
 && apt-get install unixodbc -y \
 && apt-get install unixodbc-dev -y \
 && apt-get install freetds-dev -y \
 && apt-get install freetds-bin -y \
 && apt-get install tdsodbc -y \
 && apt-get install --reinstall build-essential -y
 
 # populate "ocbcinst.ini" as this is where ODBC driver config sits
RUN echo "[FreeTDS]\n\
Description = FreeTDS Driver\n\
Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

RUN apt-get update && apt-get upgrade -y

RUN pip install --upgrade pip     

RUN mkdir /opt/app

WORKDIR /opt/app

COPY ./ ./

RUN pip install -r ./requirements.txt