#!/bin/bash
useradd redis
chown -R redis /data
if [[ -n $REDIS_PASSWORD_FILE ]]; then
	password_aux=`cat ${REDIS_PASSWORD_FILE}`
	export REDIS_PASSWORD=$password_aux
fi
if [[ ! -f /opt/bitnami/redis/etc/master.conf ]];then
	cp /opt/bitnami/redis/mounted-etc/master.conf /opt/bitnami/redis/etc/master.conf
fi
if [[ ! -f /opt/bitnami/redis/etc/redis.conf ]];then
	cp /opt/bitnami/redis/mounted-etc/redis.conf /opt/bitnami/redis/etc/redis.conf
fi
ARGS=("--port" "${REDIS_PORT}")
ARGS+=("--requirepass" "${REDIS_PASSWORD}")
ARGS+=("--masterauth" "${REDIS_PASSWORD}")
ARGS+=("--include" "/opt/bitnami/redis/etc/redis.conf")
ARGS+=("--include" "/opt/bitnami/redis/etc/master.conf")
exec /run.sh "${ARGS[@]}"