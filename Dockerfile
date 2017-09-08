FROM registry.opensource.zalan.do/stups/python:latest

RUN apt-get update && apt-get install -y python3-dev libffi-dev libssl-dev libpq-dev

COPY . /agent

WORKDIR /agent

RUN python setup.py install

RUN adduser --disabled-password --gecos '' zmon-agent

RUN pip3 install -U setuptools -e git+https://github.com/zalando-zmon/opentracing-utils.git#egg=opentracing_utils

RUN pip3 install instana

ADD scm-source.json /scm-source.json

USER zmon-agent

CMD ["bash", "start.sh"]

