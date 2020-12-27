FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/FileConnector/master/docker/global.properties conf/rtdiconfig/FileConnector/
ADD https://github.com/rtdi/FileConnector/releases/latest/download/fileconnector.war ./webapps/
ADD docker/CENSUS_2018.csv /data/uscensus/

EXPOSE 8080
EXPOSE 8443


CMD ["appcontainer.sh"]
