FROM ubuntu:latest

RUN apt-get -y update

RUN apt-get install -y software-properties-common

RUN add-apt-repository -y ppa:ethereum/ethereum

RUN apt-get install -y ethereum

RUN apt-get -y install git

RUN apt-get -y install python3.8

RUN apt-get -y install python3-pip

RUN pip install -e git+git://github.com/TimofeySugaipov/ethereum-etl.git#egg=ethereumetl

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt
