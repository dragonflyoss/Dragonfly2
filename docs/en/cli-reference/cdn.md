## CDN

CDN is a long-running process which caches downloaded data from source to avoid downloading the same files from source repeatedly

## Try it
```
go run cmd/cdnsystem/main.go [Option]
```
## Log configuration
set environment variable console=true if you want to print logs to Terminal

## Runtime metrics monitoring 
```
go run cmd/cdnsystem/main.go --profiler
```
### Options

```
      --config string    the path of configuration file with yaml extension name, default is /Users/${USER_HOME}/.dragonfly/config/cdn.yaml, it can also be 
      set by env var:CDN_CONFIG，The settings and uses of each configuration item can refer to cdn.yaml in config directory
      --console          whether logger output records to the stdout
  -h, --help             help for cdn
      --jaeger string    jaeger endpoint url, like: http://localhost:14250/api/traces
      --pprof-port int   listen port for pprof, 0 represents random port (default -1)
      --verbose          whether logger use debug level
```
