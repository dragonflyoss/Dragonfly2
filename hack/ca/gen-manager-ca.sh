#!/bin/bash

cat << EOF > manager.conf
[ ca ]
# man ca
default_ca = CA_default

[ CA_default ]
# Directory and file locations.
dir              = `pwd`/manager
certs            = \$dir/certs
crl_dir          = \$dir/crl
new_certs_dir    = \$dir/newcerts
database         = \$dir/db/index
serial           = \$dir/db/serial
RANDFILE         = \$dir/private/random
# The root key and root certificate.
private_key      = \$dir/private/manager.key.pem
certificate      = \$dir/certs/manager.cert.pem
# For certificate revocation lists.
crlnumber        = \$dir/db/crlnumber
crl              = \$dir/crl/manager.crl.pem
crl_extensions   = crl_ext
default_crl_days = 30
default_md       = sha256
name_opt         = ca_default
cert_opt         = ca_default
default_days     = 3750
preserve         = no
policy           = policy_loose

[ policy_loose ]
# Allow the intermediate CA to sign a more diverse range of certificates.
# See the POLICY FORMAT section of the ca man page.
countryName            = optional
stateOrProvinceName    = optional
localityName           = optional
organizationName       = optional
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
commonName_default          = Dragonfly Test Manager CA

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

if [ ! -d manager/certs ]; then
    mkdir -p manager/certs
fi

if [ ! -d manager/db ]; then
    mkdir -p manager/db
    touch manager/db/index
    openssl rand -hex 16 > manager/db/serial
    echo 1001 > manager/db/crlnumber
fi

if [ ! -d manager/private ]; then
    mkdir -p manager/private
    chmod 700 manager/private
fi

if [ ! -d manager/crl ]; then
    mkdir -p manager/crl
fi

if [ ! -d manager/csr ]; then
    mkdir -p manager/csr
fi

if [ ! -d manager/newcerts ]; then
    mkdir -p manager/newcerts
fi

echo generate manager ca key
openssl genrsa -out ./manager/private/manager.key.pem 4096

echo generate manager ca csr
openssl req -batch -new \
    -config manager.conf \
    -sha256 \
    -key ./manager/private/manager.key.pem \
    -out ./manager/csr/manager.csr.pem

echo review manager ca csr
openssl req -in ./manager/csr/manager.csr.pem -noout -text

echo issue manager ca cert
openssl ca -batch -config rootca.conf \
    -extensions v3_intermediate_ca \
    -days 3650 -md sha256 \
    -in ./manager/csr/manager.csr.pem \
    -out ./manager/certs/manager.cert.pem

echo merge root ca cert
cat ./manager/certs/manager.cert.pem rootca/certs/rootca.cert.pem > ./manager/certs/manager.cert-chain.pem

echo review manager cert
openssl x509 -in ./manager/certs/manager.cert.pem -noout -text
