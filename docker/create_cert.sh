openssl req -x509 -newkey rsa:4096 -keyout localhost-rsa-key.pem -out localhost-rsa-cert.pem -days 36500 -pass:changeit -subj "/C=/ST=/L=/O=rtdi.io/CN=$SSLHOSTNAME"
