FROM alpine:latest

RUN mkdir /opt/hyperscrape_worker/
COPY ./*.py /opt/hyperscrape_worker/
COPY ./requirements.txt /opt/hyperscrape_worker/requirements.txt
COPY ./config.toml /opt/hyperscrape_worker/config.toml
WORKDIR /opt/hyperscrape_worker/
RUN apk update
RUN apk add python3 py3-pip
RUN pip install -r ./requirements.txt --break-system-packages --root-user-action=ignore
CMD python main.py