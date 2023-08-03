% DFCACHE(1) Version v2.1.0 | Frivolous "Dfcache" Documentation

# NAME

**dfcache stat** — stat checks if a file exists in P2P cache system

# SYNOPSIS

Stat checks if a file exists in P2P cache system.

```shell
dfcache stat <-i cid> [flags]
```

## OPTIONS

```shell
  -i, --cid string            content or cache ID, e.g. sha256 digest of the content
      --config string         the path of configuration file with yaml extension name, default is /etc/dragonfly/dfcache.yaml, it can also be set by env var: DFCACHE_CONFIG
      --console               whether logger output records to the stdout
      --jaeger string         jaeger endpoint url, like: http://localhost:14250/api/traces
      --logdir string         Dfcache log directory
      --pprof-port int        listen port for pprof, 0 represents random port (default -1)
      --service-name string   name of the service for tracer (default "dragonfly-dfcache")
  -t, --tag string            different tags for the same cid will be recognized as different  files in P2P network
      --timeout duration      Timeout for this cache operation, 0 is infinite
      --verbose               whether logger use debug level
      --workhome string       Dfcache working directory
  -h, --help    help for stat
  -l, --local   only check task exists locally, and don't check other peers in P2P network
```

# SEE ALSO

- [dfcache](dfcache.md) - the P2P cache client of dragonfly
