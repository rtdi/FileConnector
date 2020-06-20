FROM tomcat:9-jdk14-openjdk-oracle

LABEL maintainer="info@rtdi.io"

ADD docker/server.xml conf/server.xml
ADD docker/localhost-rsa-cert.pem conf/security/ssl/
ADD docker/localhost-rsa-key.pem conf/security/ssl/
ADD docker/tomcat-users.xml conf/security/
ADD https://github.com/SAP/openui5/releases/download/1.78.1/openui5-runtime-1.78.1.zip ./webapps/openui5.war
ADD docker/global.properties conf/rtdiconfig/FileConnector/
ADD docker/context.xml conf/
ADD docker/create_cert.sh ./
ADD target/fileconnector.war ./webapps/
ADD docker/index.html ./webapps/ROOT/
ADD docker/Home.view.xml ./webapps/ROOT/
ADD docker/Home.controller.js ./webapps/ROOT/
ADD docker/applicationtile.fragment.xml ./webapps/ROOT/ui5libs/fragments/
ADD docker/apps.json ./webapps/ROOT/

EXPOSE 8080
EXPOSE 8443


CMD ["catalina.sh", "run"]
