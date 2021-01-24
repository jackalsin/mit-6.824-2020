FROM golang:1.14
RUN apt-get update
RUN apt-get install -y wget

RUN mkdir /home/Go
WORKDIR /home/Go