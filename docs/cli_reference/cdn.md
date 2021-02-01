## cdn

the data cache server of Dragonfly used for avoiding downloading the same files from source repeatedly

### Synopsis

CDN is a long-running process which caches downloaded data from source to avoid downloading the same files from source repeatedly

```
cdn [flags]
```

### Options

```
        --advertise-ip string             the cdn node ip is the ip we advertise to other peers in the p2p-network
        --clean-ratio int                 CleanRatio is the ratio to clean the disk and it is based on 10. the value of CleanRatio should be [1-10] (default 1)
        --config string                   the path of cdn configuration file (default "/etc/dragonfly/cdn.yml")
        --download-port int               downloadPort is the port for download files from cdnNode (default 8001)
        --fail-access-interval duration   fail access interval is the interval time after failed to access the URL (default 3m0s)
        --gc-disk-interval duration       gc disk interval is the interval time to execute GC disk. (default 15s)
        --gc-initial-delay duration       gc initial delay is the delay time from the start to the first GC execution (default 6s)
        --gc-meta-interval duration       gc meta interval is the interval time to execute the GC meta (default 2m0s)
    -h, --help                            help for cdn
        --home-dir string                 homeDir is the working directory of cdnNode (default "/Users/su*__*nweipeng1/cdn-system")
        --max-bandwidth rate              network rate that cdnNode can use (default 200MB)
        --port int                        listenPort is the port that cdn server listens on (default 8003)
        --profiler                        profiler sets whether cdnNode HTTP server setups profiler
        --system-bandwidth rate           network rate reserved for system (default 20MB)
        --task-expire-time duration       task expire time is the time that a task is treated expired if the task is not accessed within the time (default 3m0s)
        --young-gc-threshold file-size    gc disk interval is the interval time to execute GC disk. (default 100GB)
```
