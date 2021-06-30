## Scheduler

Scheduler is a long-running process which receives and manages download tasks from the client, notify the CDN to return to the source, 
generate and maintain a P2P network during the download process, and push suitable download nodes to the client
## Try it
```
go run cmd/scheduler/main.go [Option]
```
## Log configuration
set environment variable DF_ACTIVE_PROFILE=local if you want to print logs to Terminal

## Runtime metrics monitoring
```
go run cmd/scheduler/main.go --profiler
```
### Options

```
      --config string    the path of configuration file with yaml extension name, default is /Users/${USER_HOME}/.dragonfly/config/scheduler.yaml, it can 
      also be set by env var:SCHEDULER_CONFIG，The settings and uses of each configuration item can refer to scheduler.yaml in config directory
      --console          whether logger output records to the stdout
  -h, --help             help for cdn
      --jaeger string    jaeger endpoint url, like: http://localhost:14250/api/traces
      --pprof-port int   listen port for pprof, 0 represents random port (default -1)
      --verbose          whether logger use debug level
```
