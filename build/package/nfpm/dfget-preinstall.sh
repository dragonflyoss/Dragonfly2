#!/bin/bash

if [[ "$D7Y_DEBUG" == "true" ]]; then
  set -x
  echo pre install with param: "$@"
fi

# stop dfget daemon before upgrading
if [[ -f "/etc/systemd/system/dfget-daemon.service" ]]; then
  if [[ -f /bin/systemctl || -f /usr/bin/systemctl ]]; then
    systemctl stop dfget-daemon.service
  fi
fi
