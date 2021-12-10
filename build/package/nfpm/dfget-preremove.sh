#!/bin/bash

if [[ "$D7Y_DEBUG" == "true" ]]; then
  set -x
  echo pre remove with param: "$@"
fi

upgrade=0

# in rpm install:
# refer: https://docs.fedoraproject.org/en-US/Fedora_Draft_Documentation/0.1/html/RPM_Guide/ch09s04s05.html
if [[ "$1" -gt 0 ]]; then
  upgrade=1
fi

# in deb install
# refer: https://www.debian.org/doc/debian-policy/ch-maintainerscripts.html
if [[ "$1" =~ "upgrade" ]]; then
  upgrade=1
fi

# not in upgrade, purge systemd service
if [[ "$upgrade" -eq 0 ]]; then
  if [[ -f /bin/systemctl || -f /usr/bin/systemctl ]]; then
    systemctl stop dfget-daemon.service; systemctl disable dfget-daemon.service
  fi
fi
