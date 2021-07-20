#!/bin/sh

set -xe

mkdir -p config

cat template/cdn.template.json > config/cdn.json
cat template/cdn.template.yaml > config/cdn.yaml
cat template/dfget.template.yaml > config/dfget.yaml

ip=$(hostname -i)
hostname=$(hostname)

sed -i "s,__IP__,$ip," config/cdn.json
sed -i "s,__IP__,$ip," config/dfget.yaml
sed -i "s,__IP__,$ip," config/cdn.yaml

sed -i "s,__HOSTNAME__,$hostname," config/cdn.json

docker-compose up -d