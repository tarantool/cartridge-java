#!/bin/bash

HOST=tarantool.io
NEW_KEY_ARG=rsa:4096
DAYS_ARG=36500
CYPHER="PBE-SHA1-RC2-40"

gen_ca() {
    local ca="${1}"
    openssl req \
        -new \
        -nodes \
        -newkey "${NEW_KEY_ARG}" \
        -keyout "${ca}.key" \
        -days "${DAYS_ARG}" \
        -x509 \
        -subj "/OU=Unknown/O=Unknown/L=Unknown/ST=unknown/C=AU" \
        -out "${ca}.crt"

}

gen_cert() {
    local cert="${1}"
    local ca="${2}"

    openssl req \
        -new \
        -nodes \
        -newkey "${NEW_KEY_ARG}" \
        -subj "/CN=${HOST}/OU=Unknown/O=Unknown/L=Unknown/ST=unknown/C=AU" \
        -keyout "${cert}.key" \
        -out "${cert}.csr"

    openssl x509 \
        -req \
        -days "${DAYS_ARG}" \
        -CAcreateserial \
        -CA "${ca}.crt" \
        -CAkey "${ca}.key" \
        -in "${cert}.csr" \
        -out "${cert}.crt"

    rm -f "${cert}.csr"
    rm -f "${ca}.srl"
}

secure_key() {
    local file="${1}"
    local pass="${2}"
    openssl pkcs8 \
        -topk8 \
        -v1 ${CYPHER} \
        -in ${file}.key \
        -out ${file}.pkcs8.key \
        -passout "pass:${pass}"
}

gen_ca ca
gen_cert server ca
gen_cert client ca
secure_key client 1q2w3e
