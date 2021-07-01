#!/bin/bash

files="build/package/nfpm/config/dfget.yaml \
client/config/peerhost.go \
client/config/peerhost_test.go
client/config/dfget.go \
client/config/testdata/config/daemon.yaml \
client/config/testdata/config/proxy.yaml \
docs/en/config/dfget.yaml \
docs/en/user-guide/proxy/containerd.md \
docs/en/user-guide/proxy/docker.md \
docs/en/user-guide/registry-mirror/cri-containerd.md \
docs/en/user-guide/registry-mirror/cri-o.md
deploy/charts/dragonfly/templates/dfdaemon/dfdaemon-configmap.yaml \
deploy/charts/dragonfly/values.yaml \
deploy/kustomize/single-cluster-native/bases/dfdaemon/daemonset.yaml \
deploy/kustomize/single-cluster-native/bases/dfdaemon/dfget.yaml \
deploy/kustomize/single-cluster-openkruise/bases/dfdaemon/dfget.yaml \
deploy/kustomize/single-cluster-openkruise/bases/dfdaemon/daemonset.yaml
"

for file in $files; do
  echo update $file
  sed -i 's/alive_time/aliveTime/g' $file
  sed -i 's/gc_interval/gcInterval/g' $file
  sed -i 's/data_dir/dataDir/g' $file
  sed -i 's/work_home/workHome/g' $file
  sed -i 's/keep_storage/keepStorage/g' $file

  sed -i 's/net_addrs/netAddrs/g' $file
  sed -i 's/schedule_timeout/scheduleTimeout/g' $file

  sed -i 's/security_domain/securityDomain/g' $file
  sed -i 's/net_topology/netTopology/g' $file
  sed -i 's/listen_ip/listenIP/g' $file
  sed -i 's/advertise_ip/advertiseIP/g' $file

  sed -i 's/total_rate_limit/totalRateLimit/g' $file
  sed -i 's/per_peer_rate_limit/perPeerRateLimit/g' $file
  sed -i 's/download_grpc/downloadGRPC/g' $file
  sed -i 's/peer_grpc/peerGRPC/g' $file
  sed -i 's/calculate_digest/calculateDigest/g' $file
  sed -i 's/basic_auth/basicAuth/g' $file
  sed -i 's/default_filter/defaultFilter/g' $file
  sed -i 's/max_concurrency/maxConcurrency/g' $file
  sed -i 's/registry_mirror/registryMirror/g' $file
  sed -i 's/white_list/whiteList/g' $file
  sed -i 's/hijack_https/hijackHTTPS/g' $file

  sed -i 's/rate_limit/rateLimit/g' $file
  sed -i 's/tcp_listen/tcpListen/g' $file
  sed -i 's/unix_listen/unixListen/g' $file

  sed -i 's/ca_cert/caCert/g' $file
  sed -i 's/tls_config/tlsConfig/g' $file

  sed -i 's/data_path/dataPath/g' $file
  sed -i 's/task_expire_time/taskExpireTime/g' $file
  sed -i 's/disk_gc_threshold/diskGCThreshold/g' $file

  sed -i 's/use_https/useHTTPS/g' $file

  sed -i 's/benchmark-rate/benchmarkRate/g' $file
  sed -i 's/digest_method/digestMethod/g' $file
  sed -i 's/digest_value/digestValue/g' $file
  sed -i 's/call_system/callSystem/g' $file
  sed -i 's/disable_back_source/disableBackSource/g' $file
  sed -i 's/show_bar/showBar/g' $file
  sed -i 's/rate-limit/rateLimit/g' $file
  sed -i 's/more_daemon_options/moreDaemonOptions/g' $file
done
