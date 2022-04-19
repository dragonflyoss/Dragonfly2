#!/bin/sh

# skip unsupported and newly kernel
if [ ! -d /sys/fs/cgroup/cpuset,cpu,cpuacct ]; then
  exit 0
fi

# fix cpu set not settled in old kernel
mkdir -p /sys/fs/cgroup/cpuset,cpu,cpuacct/dragonfly.slice/dfget-daemon.service
cat /sys/fs/cgroup/cpuset,cpu,cpuacct/cpuset.cpus > \
  /sys/fs/cgroup/cpuset,cpu,cpuacct/dragonfly.slice/cpuset.cpus
cat /sys/fs/cgroup/cpuset,cpu,cpuacct/cpuset.mems > \
  /sys/fs/cgroup/cpuset,cpu,cpuacct/dragonfly.slice/cpuset.mems
cat /sys/fs/cgroup/cpuset,cpu,cpuacct/cpuset.cpus > \
  /sys/fs/cgroup/cpuset,cpu,cpuacct/dragonfly.slice/dfget-daemon.service/cpuset.cpus
cat /sys/fs/cgroup/cpuset,cpu,cpuacct/cpuset.mems > \
  /sys/fs/cgroup/cpuset,cpu,cpuacct/dragonfly.slice/dfget-daemon.service/cpuset.mems
