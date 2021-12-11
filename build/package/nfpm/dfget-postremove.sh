#!/bin/bash

if [[ "$D7Y_DEBUG" == "true" ]]; then
  set -x
  echo post remove with param: "$@"
fi

if [[ -f /bin/systemctl || -f /usr/bin/systemctl ]]; then
  systemctl daemon-reload
fi
