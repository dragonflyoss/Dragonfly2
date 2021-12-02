#!/bin/bash
useradd redis
chown -R redis /data
if [[ -n $REDIS_PASSWORD_FILE ]]; then
	password_aux=`cat ${REDIS_PASSWORD_FILE}`
	export REDIS_PASSWORD=$password_aux
fi
if [[ -n $REDIS_MASTER_PASSWORD_FILE ]]; then
	password_aux=`cat ${REDIS_MASTER_PASSWORD_FILE}`
	export REDIS_MASTER_PASSWORD=$password_aux
fi
if [[ ! -f /opt/bitnami/redis/etc/replica.conf ]];then
	cp /opt/bitnami/redis/mounted-etc/replica.conf /opt/bitnami/redis/etc/replica.conf
fi
if [[ ! -f /opt/bitnami/redis/etc/redis.conf ]];then
	cp /opt/bitnami/redis/mounted-etc/redis.conf /opt/bitnami/redis/etc/redis.conf
fi
ARGS=("--port" "${REDIS_PORT}")
ARGS+=("--slaveof" "${REDIS_MASTER_HOST}" "${REDIS_MASTER_PORT_NUMBER}")
ARGS+=("--requirepass" "${REDIS_PASSWORD}")
ARGS+=("--masterauth" "${REDIS_MASTER_PASSWORD}")
ARGS+=("--include" "/opt/bitnami/redis/etc/redis.conf")
ARGS+=("--include" "/opt/bitnami/redis/etc/replica.conf")
exec /run.sh "${ARGS[@]}"