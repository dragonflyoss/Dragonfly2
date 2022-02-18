# Development

Easily set up a local development environment.

## Step 1: Install docker and docker compose

See documentation on [docs.docker.com]

## Step 2: Start dragonfly

Enter dragonfly documentation and start docker-compose.

```bash
$ docker-compose up
Creating network "dragonfly2_default" with the default driver
Creating cdn ... done
Creating scheduler ... done
Creating dfdaemon  ... done
Attaching to cdn, scheduler, dfdaemon
```

## Step 3: Log analysis

Show dragonfly logs.

<!-- markdownlint-disable -->

```bash
$ tail -f log/**/*.log
==> log/dragonfly/cdn/core.log <==
{"level":"info","ts":"2021-02-26 05:43:36.896","caller":"cmd/root.go:90","msg":"success to init local ip of cdn, start to run cdn system, use ip: 172.21.0.2"}
{"level":"info","ts":"2021-02-26 05:43:36.901","caller":"plugins/plugins.go:51","msg":"add plugin[storage][local]"}
{"level":"info","ts":"2021-02-26 05:43:36.902","caller":"plugins/plugins.go:37","msg":"plugin[sourceclient][http] is disabled"}

==> log/dragonfly/dfdaemon/core.log <==
{"level":"info","ts":"2021-02-26 05:43:37.797","caller":"proxy/proxy_manager.go:63","msg":"registry mirror: https://index.docker.io"}
{"level":"info","ts":"2021-02-26 05:43:37.798","caller":"proxy/proxy_manager.go:68","msg":"load 1 proxy rules"}
{"level":"info","ts":"2021-02-26 05:43:37.799","caller":"proxy/proxy_manager.go:78","msg":"[1] proxy blobs/sha256.* with dragonfly "}
{"level":"debug","ts":"2021-02-26 05:43:37.799","caller":"rpc/server_listen.go:31","msg":"start to listen port: 0.0.0.0:65000"}
{"level":"debug","ts":"2021-02-26 05:43:37.800","caller":"rpc/server_listen.go:31","msg":"start to listen port: 0.0.0.0:65002"}
{"level":"debug","ts":"2021-02-26 05:43:37.800","caller":"rpc/server_listen.go:31","msg":"start to listen port: 0.0.0.0:65001"}
{"level":"info","ts":"2021-02-26 05:43:37.800","caller":"daemon/peerhost.go:274","msg":"serve download grpc at unix:///tmp/dfdamon.sock"}
{"level":"info","ts":"2021-02-26 05:43:37.801","caller":"daemon/peerhost.go:285","msg":"serve peer grpc at tcp://[::]:65000"}
{"level":"info","ts":"2021-02-26 05:43:37.801","caller":"daemon/peerhost.go:320","msg":"serve upload service at tcp://[::]:65002"}
{"level":"info","ts":"2021-02-26 05:43:37.801","caller":"daemon/peerhost.go:306","msg":"serve proxy at tcp://0.0.0.0:65001"}

==> log/dragonfly/scheduler/core.log <==
{"level":"info","ts":"2021-02-26 05:43:37.332","caller":"cmd/root.go:57","msg":"start to run scheduler"}
{"level":"info","ts":"2021-02-26 05:43:37.338","caller":"server/server.go:35","msg":"start server at port %!s(int=8002)"}
{"level":"info","ts":"2021-02-26 05:43:37.342","caller":"worker/sender.go:49","msg":"start sender worker : 50"}
{"level":"info","ts":"2021-02-26 05:43:37.343","caller":"worker/worker_group.go:64","msg":"start scheduler worker number:6"}
```

<!-- markdownlint-restore -->

## Step 4: Stop dragonfly

```bash
$ docker-compose down
Removing dfdaemon  ... done
Removing scheduler ... done
Removing cdn       ... done
Removing network dragonfly2_default
```

[docs.docker.com]: https://docs.docker.com
