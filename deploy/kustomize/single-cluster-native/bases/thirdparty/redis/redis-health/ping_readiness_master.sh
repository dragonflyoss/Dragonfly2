#!/bin/bash
no_auth_warning=$([[ "$(redis-cli --version)" =~ (redis-cli 5.*) ]] && echo --no-auth-warning)
	response=$(
	timeout -s 3 $1 \
	redis-cli \
	-a $REDIS_MASTER_PASSWORD $no_auth_warning \
	-h $REDIS_MASTER_HOST \
	-p $REDIS_MASTER_PORT_NUMBER \
	ping
)
if [ "$response" != "PONG" ]; then
	echo "$response"
	exit 1
fi