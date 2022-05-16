#!/bin/sh

set -e

REPO=${REPO:-d7yio}
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

run_container(){
    RUNTIME=${RUNTIME:-docker}
    echo use container runtime: ${RUNTIME}

    echo try to clean old containers
    ${RUNTIME} rm -f dragonfly-manager dragonfly-scheduler dragonfly-dfdaemon

    printf "create dragonfly-manager "
    ${RUNTIME} run -d --name dragonfly-manager --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/manager.yaml:/etc/dragonfly/manager.yaml \
        ${REPO}/manager:${TAG}

    printf "create dragonfly-seed-peer "
    ${RUNTIME} run -d --name dragonfly-seed-peer --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/seed-peer.yaml:/etc/dragonfly/dfget.yaml \
        ${REPO}/dfdaemon:${TAG}

    printf "create dragonfly-scheduler "
    ${RUNTIME} run -d --name dragonfly-scheduler --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/scheduler.yaml:/etc/dragonfly/scheduler.yaml \
        ${REPO}/scheduler:${TAG}

    printf "create dragonfly-dfdaemon "
    ${RUNTIME} run -d --name dragonfly-dfdaemon --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/dfget.yaml:/etc/dragonfly/dfget.yaml \
        ${REPO}/dfdaemon:${TAG}
}

prepare

case "$1" in
  container)
    run_container
    ;;

  *)
    if [ -z "$1" ]; then

        # start all of docker-compose defined service
        docker-compose up -d

        # docker-compose version 3 depends_on does not wait for redis and mysql to be “ready” before starting manager ...
        # doc https://docs.docker.com/compose/compose-file/compose-file-v3/#depends_on
        for i in $(seq 0 10); do
          service_num=$(docker-compose ps --services |wc -l)
          ready_num=$(docker-compose ps | grep healthy | wc -l)
           if [ "$service_num" -eq "$ready_num" ]; then
             break
           fi
           echo "wait for all service ready: $ready_num/$service_num,$i times check"
           sleep 2
        done

        # print service list info
        docker-compose ps
        exit 0
    fi
    echo "unknown argument: $1"
    exit 1
    ;;
esac
