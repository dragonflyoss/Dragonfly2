#!/bin/bash
no_auth_warning=$([[ "$(redis-cli --version)" =~ (redis-cli 5.*) ]] && echo --no-auth-warning)
response=$(
	timeout -s 3 $1 \
	redis-cli \
	-a $REDIS_PASSWORD $no_auth_warning \
	-h localhost \
	-p $REDIS_PORT \
	ping
)
if [ "$response" != "PONG" ] && [ "$response" != "LOADING Redis is loading the dataset in memory" ]; then
	echo "$response"
	exit 1
fi