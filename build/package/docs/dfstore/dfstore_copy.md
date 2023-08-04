% DFCACHE(1) Version v2.1.0 | Frivolous "Dfstore" Documentation

# NAME

**dfstore cp** — copies a local file or dragonfly object to another location locally or in dragonfly object storage

# SYNOPSIS

Copies a local file or dragonfly object to another location locally or in dragonfly object storage.

```shell
dfstore cp <source> <target> [flags]
```

## OPTIONS

```shell
      --filter string      filter is used to generate a unique task id by filtering unnecessary query params in the URL, it is separated by & character
  -h, --help               help for cp
      --max-replicas int   maxReplicas is the maximum number of replicas of an object cache in seed peers (default 3)
  -m, --mode int           mode is the mode in which the backend is written, when the value is 0, it represents AsyncWriteBack, and when the value is 1, it represents WriteBack
  -e, --endpoint string   endpoint of object storage service (default "http://127.0.0.1:65004")
```

# SEE ALSO

- [dfstore](dfstore.md) - object storage client of dragonfly
