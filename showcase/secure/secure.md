





openssl genrsa -des3 -passout pass:x -out server.pass.key 2048
openssl rsa -passin pass:x -in server.pass.key -out server.key
rm server.pass.key
openssl req -new -key server.key -out server.csr

vi v3.ext
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names
 
[alt_names]
DNS.1 = brainiac.com



openssl x509 -req -sha256 -extfile v3.ext -days 365 -in server.csr -signkey server.key -out server.crt





keytool -genkey -alias server -keyalg RSA -keystore server.jks -keysize 2048


keytool -certreq -alias server -keystore server.jks -file server.csr

keytool -import -trustcacerts -alias server -file server.txt -keystore server.jks
