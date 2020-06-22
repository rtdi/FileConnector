FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/RTDIFileConnector/master/docker/global.properties conf/rtdiconfig/FileConnector/
ADD https://github.com/rtdi/RTDIFileConnector/releases/latest/download/fileconnector.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["catalina.sh", "run"]
