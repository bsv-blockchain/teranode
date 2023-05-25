#!/usr/bin/env bash

# Generate self-signed certificates for testing Aerospike with TLS enabled.

PREFIX='example'
PASSWORD='example'
ORG='Bitcoin Association'
VALID_DAYS=365
CERT_DIR="certs"
AEROSPIKE_DIR="aerospike/etc"

# create subdirectory to store certs
echo "Creating '${CERT_DIR}' directory"
mkdir -p $CERT_DIR

# create CA cert
echo "Creating CA certificate"
openssl req -new -x509 -days ${VALID_DAYS} -extensions v3_ca -passout pass:"$PASSWORD" -keyout ${CERT_DIR}/${PREFIX}.ca.key -out ${CERT_DIR}/${PREFIX}.ca.crt -subj "/CN=${PREFIX}.ca/O=$ORG/C=US"

# create ECDSA certs
CERTS=( "${PREFIX}.client" "${PREFIX}.server" )
for CERT in "${CERTS[@]}"
do
    echo "Creating ${CERT} ECDSA certificate"
    openssl ecparam -genkey -out ${CERT_DIR}/ecparam.pem -name prime256v1
    openssl genpkey -paramfile ${CERT_DIR}/ecparam.pem -out ${CERT_DIR}/${CERT}.key
    openssl req -new -key ${CERT_DIR}/${CERT}.key -out ${CERT_DIR}/${CERT}.csr -subj "/CN=${CERT}/O=$ORG/C=US"
    openssl x509 -req -days ${VALID_DAYS} -passin pass:"$PASSWORD" -in ${CERT_DIR}/${CERT}.csr -CA ${CERT_DIR}/${PREFIX}.ca.crt -CAkey ${CERT_DIR}/${PREFIX}.ca.key -CAcreateserial -out ${CERT_DIR}/${CERT}.crt
    rm ${CERT_DIR}/ecparam.pem ${CERT_DIR}/${CERT}.csr ${CERT_DIR}/${PREFIX}.ca.srl
done

echo "Copying server certificate to aerospike server config directory"
mkdir -p aerospike/etc/certs/
mkdir -p aerospike/etc/private/
cp ${CERT_DIR}/${PREFIX}.ca.crt ${AEROSPIKE_DIR}/certs/
cp ${CERT_DIR}/${PREFIX}.server.crt ${AEROSPIKE_DIR}/certs/
cp ${CERT_DIR}/${PREFIX}.server.key ${AEROSPIKE_DIR}/private/

echo "---"
tree ${CERT_DIR} ${AEROSPIKE_DIR}/certs/ ${AEROSPIKE_DIR}/private/