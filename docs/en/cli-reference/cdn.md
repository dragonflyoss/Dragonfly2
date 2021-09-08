# CDN

CDN is a long-running process which caches downloaded data from source to avoid downloading the same files from source repeatedly

## Usage

```
cdn [flags]
cdn [command]
```

## Available Commands:

```
doc         generate documents 
help        Help about any command
version     show version
```

## Flags

```
    --config string         the path of configuration file with yaml extension name, default is /Users/${USER_HOME}/.dragonfly/config/cdn.yaml, it can 
      also be set by environment variable cdn_config
    --console               whether logger output records to the stdout
    -h, --help              help for scheduler
    --jaeger string         jaeger endpoint url, like: http://localhost:14250/api/traces
    --pprof-port int        listen port for pprof, 0 represents random port (default -1)
    --service-name string   name of the service for tracer (default "dragonfly-cdn")
    --verbose               whether logger use debug level
```
