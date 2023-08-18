FROM python:3.11.0

ENTRYPOINT [ "bash" ]

WORKDIR /app

COPY code code/

RUN pip install -r /app/code/requirements.txt

#os tools install and update
RUN apt update
RUN apt-get install sudo
RUN apt-get update && apt-get install -y lsb-release && apt-get clean all
RUN apt-get install apt-utils