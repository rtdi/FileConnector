FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD docker/global.properties conf/rtdiconfig/FileConnector/
ADD target/fileconnector.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["catalina.sh", "run"]
