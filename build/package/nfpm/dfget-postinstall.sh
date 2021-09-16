#!/bin/bash

chmod a+s /usr/bin/dfget

which systemctl && (systemctl daemon-reload; systemctl enable dfget-daemon.service)
