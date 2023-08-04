% DFCACHE(1) Version v2.1.0 | Frivolous "Dfcache" Documentation

# NAME

**dfcache** — the P2P cache client of dragonfly

# SYNOPSIS

dfcache is the cache client to of dragonfly that communicates with dfdaemon and operates
on files in P2P network, where the P2P network acts as a cache system.

The difference between dfcache and dfget is that, dfget downloads file from a given URL,
the file might be on other peers in P2P network or a seed peer, it's the P2P network's
responsibility to download file from source; but dfcache could only export or download a
file that has been imported or added into P2P network by other peer, it's the user's
responsibility to go back to source and add file into P2P network.

## OPTIONS

```
  -i, --cid string            content or cache ID, e.g. sha256 digest of the content
      --config string         the path of configuration file with yaml extension name, default is /etc/dragonfly/dfcache.yaml, it can also be set by env var: DFCACHE_CONFIG
      --console               whether logger output records to the stdout
  -h, --help                  help for dfcache
      --jaeger string         jaeger endpoint url, like: http://localhost:14250/api/traces
      --logdir string         Dfcache log directory
      --pprof-port int        listen port for pprof, 0 represents random port (default -1)
      --service-name string   name of the service for tracer (default "dragonfly-dfcache")
  -t, --tag string            different tags for the same cid will be recognized as different  files in P2P network
      --timeout duration      Timeout for this cache operation, 0 is infinite
      --verbose               whether logger use debug level
      --workhome string       Dfcache working directory
```

# SEE ALSO

- [dfcache completion](dfcache_completion.md) - generate the autocompletion script for the specified shell
- [dfcache delete](dfcache_delete.md) - delete file from P2P cache system
- [dfcache doc](dfcache_doc.md) - generate documents
- [dfcache export](dfcache_export.md) - export file from P2P cache system
- [dfcache import](dfcache_import.md) - import file into P2P cache system
- [dfcache plugin](dfcache_plugin.md) - show plugin
- [dfcache stat](dfcache_stat.md) - stat checks if a file exists in P2P cache system
- [dfcache version](dfcache_version.md) - show version

# BUGS

See GitHub Issues: <https://github.com/dragonflyoss/Dragonfly2/issues>
