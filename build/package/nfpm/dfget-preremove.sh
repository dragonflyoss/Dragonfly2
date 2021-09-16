#!/bin/bash

which systemctl && (systemctl stop dfget-daemon.service; systemctl disable dfget-daemon.service)
