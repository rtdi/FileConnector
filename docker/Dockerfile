FROM tomcat:9-jdk14-openjdk-oracle

LABEL maintainer="info@rtdi.io"

ADD docker/server.xml conf/server.xml
ADD docker/localhost-rsa-cert.pem conf/security/ssl/
ADD docker/localhost-rsa-key.pem conf/security/ssl/
ADD docker/topcat-users.xml conf/security/
ADD https://github.com/SAP/openui5/releases/download/1.74.0/openui5-runtime-1.74.0.zip ./webapps/openui5.war
ADD target/HanaAppContainer.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["catalina.sh", "run"]
