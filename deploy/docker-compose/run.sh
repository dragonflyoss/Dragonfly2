#!/bin/sh

set -e

REPO=${REPO:-dragonflyoss}
TAG=${TAG:-latest}

DIR=$(cd "$(dirname "$0")" && pwd)
cd $DIR

prepare(){
    mkdir -p config

    ip=${IP:-$(hostname -i)}

    sed "s,__IP__,$ip," template/dfget.template.yaml > config/dfget.yaml
    sed "s,__IP__,$ip," template/seed-peer.template.yaml > config/seed-peer.yaml
    sed "s,__IP__,$ip," template/scheduler.template.yaml > config/scheduler.yaml
    sed "s,__IP__,$ip," template/manager.template.yaml > config/manager.yaml
}

delete_container(){
    RUNTIME=${RUNTIME:-docker}
    echo use container runtime: ${RUNTIME}

    echo try to clean old containers
    ${RUNTIME} rm -f dragonfly-redis dragonfly-mysql dragonfly-manager dragonfly-scheduler \
        dragonfly-dfdaemon dragonfly-seed-peer
}

run_container(){
    RUNTIME=${RUNTIME:-docker}
    echo use container runtime: ${RUNTIME}

    echo try to clean old containers
    ${RUNTIME} rm -f dragonfly-redis dragonfly-mysql dragonfly-manager dragonfly-scheduler \
        dragonfly-dfdaemon dragonfly-seed-peer

    printf "create dragonfly-redis "
    ${RUNTIME} run -d --name dragonfly-redis --restart=always -p 6379:6379 \
        redis:6-alpine \
        --requirepass "dragonfly"

    printf "create dragonfly-mysql "
    ${RUNTIME} run -d --name dragonfly-mysql --restart=always -p 3306:3306 \
        --env MARIADB_USER="dragonfly" \
        --env MARIADB_PASSWORD="dragonfly" \
        --env MARIADB_DATABASE="manager" \
        --env MARIADB_ALLOW_EMPTY_ROOT_PASSWORD="yes" \
        mariadb:10.6

    printf "create dragonfly-manager "
    ${RUNTIME} run -d --name dragonfly-manager --restart=always --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/manager.yaml:/etc/dragonfly/manager.yaml \
        ${REPO}/manager:${TAG}

    printf "create dragonfly-seed-peer "
    ${RUNTIME} run -d --name dragonfly-seed-peer --restart=always --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/seed-peer.yaml:/etc/dragonfly/dfget.yaml \
        ${REPO}/dfdaemon:${TAG}

    printf "create dragonfly-scheduler "
    ${RUNTIME} run -d --name dragonfly-scheduler --restart=always --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/scheduler.yaml:/etc/dragonfly/scheduler.yaml \
        ${REPO}/scheduler:${TAG}

    printf "create dragonfly-dfdaemon "
    ${RUNTIME} run -d --name dragonfly-dfdaemon --restart=always --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/dfget.yaml:/etc/dragonfly/dfget.yaml \
        ${REPO}/dfdaemon:${TAG}
}

prepare

case "$1" in
  container)
    run_container
    ;;
  delete_container)
    delete_container
    ;;
  *)
    if [ -z "$1" ]; then
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
    fi
    echo "unknown argument: $1"
    exit 1
    ;;
esac
