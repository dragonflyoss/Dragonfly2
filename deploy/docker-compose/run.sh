#!/bin/sh

set -e

REPO=${REPO:-d7yio}
TAG=${TAG:-latest}

DIR=$(cd "$(dirname "$0")" && pwd)
cd $DIR

prepare(){
    mkdir -p config

    cat template/cdn.template.json > config/cdn.json
    cat template/cdn.template.yaml > config/cdn.yaml
    cat template/dfget.template.yaml > config/dfget.yaml

    ip=${IP:-$(hostname -i)}
    hostname=$(hostname)

    sed -i "s,__IP__,$ip," config/cdn.json
    sed -i "s,__IP__,$ip," config/dfget.yaml
    sed -i "s,__IP__,$ip," config/cdn.yaml

    sed -i "s,__HOSTNAME__,$hostname," config/cdn.json
}

run_container(){
    RUNTIME=${RUNTIME:-docker}
    echo use container runtime: ${RUNTIME}

    echo try to clean old containers
    ${RUNTIME} rm -f dragonfly-cdn dragonfly-scheduler dragonfly-dfdaemon

    printf "create dragonfly-cdn "
    ${RUNTIME} run -d --name dragonfly-cdn --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/cdn.yaml:/etc/dragonfly/cdn.yaml \
        -v ${DIR}/config/nginx.conf:/etc/nginx/nginx.conf \
        ${REPO}/cdn:${TAG}

    printf "create dragonfly-scheduler "
    ${RUNTIME} run -d --name dragonfly-scheduler --net=host \
        -v /tmp/log/dragonfly:/var/log/dragonfly \
        -v ${DIR}/config/scheduler.yaml:/etc/dragonfly/scheduler.yaml \
        -v ${DIR}/config/cdn.json:/opt/dragonfly/scheduler-cdn/cdn.json \
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
        docker-compose up -d
        exit 0
    fi
    echo "unknown argument: $1"
    exit 1
    ;;
esac
