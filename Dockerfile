# Based on the boot image
FROM 10.122.18.18:8082/healthcheck/smart_storage_dagster
MAINTAINER niezy4 <niezy4@lenovo.com>
ENV LANG en_US.UTF-8
RUN rm -rf /data/project/test
RUN mkdir -p /data/project/test
COPY / /data/project/test/
RUN cd data/project/test/ && pip install -r requirements.txt -i http://10.122.18.18:8081/repository/pypi-all/simple --trusted-host 10.122.18.18
