#!/bin/sh

set -e

REPO=${REPO:-dragonflyoss}
TAG=${TAG:-latest}
CLIENT_TAG=${CLIENT_TAG:-latest}

DIR=$(cd "$(dirname "$0")" && pwd)
cd $DIR

prepare(){
    mkdir -p config

    ip=${IP:-$(hostname -i)}

    sed "s,__IP__,$ip," template/client.template.yaml > config/client.yaml
    sed "s,__IP__,$ip," template/seed-client.template.yaml > config/seed-client.yaml
    sed "s,__IP__,$ip," template/scheduler.template.yaml > config/scheduler.yaml
    sed "s,__IP__,$ip," template/manager.template.yaml > config/manager.yaml
}

run() {
  # start all of docker-compose defined service
  COMPOSE=docker-compose
  # use docker compose plugin
  if docker compose version; then
    COMPOSE="docker compose"
  fi

  $COMPOSE up -d

  # docker-compose version 3 depends_on does not wait for redis and mysql to be “ready” before starting manager ...
  # doc https://docs.docker.com/compose/compose-file/compose-file-v3/#depends_on
  for i in $(seq 0 10); do
    service_num=$($COMPOSE ps --services |wc -l)
    ready_num=$($COMPOSE ps | grep healthy | wc -l)
     if [ "$service_num" -eq "$ready_num" ]; then
       break
     fi
     echo "wait for all service ready: $ready_num/$service_num,$i times check"
     sleep 2
  done

  # print service list info
  $COMPOSE ps
  exit 0
}

prepare
run
