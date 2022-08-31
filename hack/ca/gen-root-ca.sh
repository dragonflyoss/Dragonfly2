#!/bin/bash

cat << EOF > rootca.conf
[ ca ]
# man ca
default_ca = CA_default

[ CA_default ]
# Directory and file locations.
dir              = `pwd`/rootca
certs            = \$dir/certs
crl_dir          = \$dir/crl
new_certs_dir    = \$dir/newcerts
database         = \$dir/db/index
serial           = \$dir/db/serial
RANDFILE         = \$dir/private/random
# The root key and root certificate.
private_key      = \$dir/private/rootca.key.pem
certificate      = \$dir/certs/rootca.cert.pem
# For certificate revocation lists.
crlnumber        = \$dir/db/crlnumber
crl              = \$dir/crl/rootca.crl.pem
crl_extensions   = crl_ext
default_crl_days = 30
default_md       = sha256
name_opt         = ca_default
cert_opt         = ca_default
default_days     = 3750
preserve         = no
policy           = policy_strict

[ policy_strict ]
# The root CA should only sign intermediate certificates that match.
# See the POLICY FORMAT section of man ca.
countryName            = match
stateOrProvinceName    = match
organizationName       = match
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional

[ req ]
default_bits        = 2048
default_keyfile     = key.pem
default_md          = sha256
distinguished_name  = req_distinguished_name
req_extensions      = req_ext
string_mask         = nombstr
x509_extensions     = v3_ca
# prompt              = no
# input_password      = 123456

[ req_distinguished_name ]
countryName                 = Country Name (2 letter code)
countryName_default         = CN
stateOrProvinceName         = State or Province Name (full name)
stateOrProvinceName_default = Zhejiang
localityName                = Locality Name (eg, city)
localityName_default        = Hangzhou
organizationName            = Organization Name (eg, company)
organizationName_default    = Dragonfly
commonName                  = Common Name (e.g. server FQDN or YOUR name)
commonName_max              = 64
commonName_default          = Dragonfly Test Root CA

[ req_ext ]
subjectKeyIdentifier = hash
basicConstraints     = CA:TRUE
keyUsage             = digitalSignature, keyEncipherment, keyCertSign, cRLSign

[ v3_ca ]
# Extensions for a typical CA (man x509v3_config).
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints       = critical, CA:TRUE
keyUsage               = critical, digitalSignature, cRLSign, keyCertSign

[ v3_intermediate_ca ]
# Extensions for a typical intermediate CA (man x509v3_config).
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints       = critical, CA:TRUE, pathlen:0
keyUsage               = critical, digitalSignature, cRLSign, keyCertSign

[ crl_ext ]
# Extension for CRLs (man x509v3_config).
authorityKeyIdentifier = keyid:always

[ ocsp ]
# Extension for OCSP signing certificates (man ocsp).
basicConstraints       = CA:FALSE
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid,issuer
keyUsage               = critical, digitalSignature
extendedKeyUsage       = critical, OCSPSigning
EOF

# create dir certs db private crl newcerts under rootca dir.
if [ ! -d rootca/certs ]; then
    mkdir -p rootca/certs
fi

if [ ! -d rootca/db ]; then
    mkdir -p rootca/db
    touch rootca/db/index
    openssl rand -hex 16 > rootca/db/serial
    echo 1001 > rootca/db/crlnumber
fi

if [ ! -d rootca/private ]; then
    mkdir -p rootca/private
    chmod 700 rootca/private
fi

if [ ! -d rootca/crl ]; then
    mkdir -p rootca/crl
fi

if [ ! -d rootca/csr ]; then
    mkdir -p rootca/csr
fi

if [ ! -d rootca/newcerts ]; then
    mkdir -p rootca/newcerts
fi

echo generate root ca key
openssl genrsa -out rootca/private/rootca.key.pem 4096

echo generate root ca csr
openssl req -batch -new \
    -config rootca.conf \
    -sha256 \
    -key rootca/private/rootca.key.pem \
    -out rootca/csr/rootca.csr.pem

openssl req -text -noout -in rootca/csr/rootca.csr.pem

echo generate root ca cert
openssl ca -batch -selfsign \
    -config rootca.conf \
    -in rootca/csr/rootca.csr.pem \
    -extensions v3_ca \
    -days 7300 \
    -out rootca/certs/rootca.cert.pem

echo generate root ca cer format cert
openssl x509 -inform PEM -in rootca/certs/rootca.cert.pem -outform DER -out rootca/certs/rootca.cert.cer

echo review ca cert
openssl x509 -in rootca/certs/rootca.cert.pem -noout -text
