# Scheduler

Scheduler is a long-running process which receives
and manages download tasks from the client,
notify the CDN to return to the source, generate and maintain a
P2P network during the download process,
and push suitable download nodes to the client

## Usage

```text
scheduler [flags]
scheduler [command]
```

## Available Commands

```text
doc         generate documents
help        Help about any command
version     show version
```

## Flags

<!-- markdownlint-disable -->

```text
    --config string         the path of configuration file with yaml extension name, default is /Users/${USER_HOME}/.dragonfly/config/scheduler.yaml, it can
                            also be set by environment variable scheduler_config
    --console               whether logger output records to the stdout
    -h, --help              help for scheduler
    --jaeger string         jaeger endpoint url, like: http://localhost:14250/api/traces
    --pprof-port int        listen port for pprof, 0 represents random port (default -1)
    --service-name string   name of the service for tracer (default "dragonfly-scheduler")
    --verbose               whether logger use debug level
```

<!-- markdownlint-restore -->
