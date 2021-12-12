#!/bin/bash

if [[ "$D7Y_DEBUG" == "true" ]]; then
  set -x
  echo post install with param: "$@"
fi

chmod a+s /usr/bin/dfget

if [[ -f /bin/systemctl || -f /usr/bin/systemctl ]]; then
  systemctl daemon-reload; systemctl enable --now dfget-daemon.service
fi
