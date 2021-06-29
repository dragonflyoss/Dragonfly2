# dfget

`dfget` is the client of Dragonfly used to download and upload files

### Synopsis

dfget is the client of Dragonfly which takes a role of peer in a P2P network. When user triggers a file downloading
task, dfget will download the pieces of file from other peers. Meanwhile, it will act as an uploader to support other
peers to download pieces from it if it owns them. In addition, dfget has the abilities to provide more advanced
functionality, such as network bandwidth limit, transmission encryption and so on.

```
dfget [flags]
```

### Example

```
dfget --schedulers 127.0.0.1:8002 -o /path/to/output -u "http://example.com/object"
```

## Log configuration

set environment variable DF_ACTIVE_PROFILE=local if you want to print logs to Terminal

### Options

```
      --alivetime duration           alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit (default 5m0s)
      --cacerts strings              the cacert file which is used to verify remote server when supernode interact with the source.
      --callsystem string            the name of dfget caller which is for debugging. Once set, it will be passed to all components around the request to make debugging easy
      --clientqueue int              specify the size of client queue which controls the number of pieces that can be processed simultaneously
      --console                      show log on console, it's conflict with '--showbar'
      --daemon-pid string            the daemon pid (default "/tmp/dfdaemon.pid")
      --daemon-sock string           the unix domain socket address for grpc with daemon (default "/tmp/dfdamon.sock")
      --dfdaemon                     identify whether the request is from dfdaemon
      --expiretime duration          caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted (default 3m0s)
  -f, --filter string                filter some query params of URL, use char '&' to separate different params
                                     eg: -f 'key&sign' will filter 'key' and 'sign' query param
                                     in this way, different but actually the same URLs can reuse the same downloading task
      --header stringArray           http header, eg: --header='Accept: *' --header='Host: abc'
  -h, --help                         help for dfget
      --home string                  the work home directory of dfget (default "/Users/jim/.dragonfly/dfdaemon/")
  -i, --identifier string            the usage of identifier is making different downloading tasks generate different downloading task IDs even if they have the same URLs. conflict with --md5.
      --insecure                     identify whether supernode should skip secure verify when interact with the source.
      --ip string                    IP address that server will listen on (default "0.0.0.0")
  -m, --md5 string                   md5 value input from user for the requested downloading file to enhance security
      --more-daemon-options string   more options passed to daemon by command line, please confirm your options with "dfget daemon --help"
  -n, --node supernodes              deprecated, please use schedulers instead. specify the addresses(host:port=weight) of supernodes where the host is necessary, the port(default: 8002) and the weight(default:1) are optional. And the type of weight must be integer
      --notbacksource                disable back source downloading for requested file when p2p fails to download it
  -o, --output string                destination path which is used to store the requested downloading file. It must contain detailed directory and specific filename, for example, '/tmp/file.mp4'
  -p, --pattern string               download pattern, must be p2p/cdn/source, cdn and source do not support flag --totallimit (default "p2p")
      --port int                     port number that server will listen on (default 65002)
      --schedulers schedulers        the scheduler addresses
  -b, --showbar                      show progress bar, it is conflict with '--console'
  -e, --timeout duration             timeout set for file downloading task. If dfget has not finished downloading all pieces of file before --timeout, the dfget will throw an error and exit
      --totallimit ratelimit         network bandwidth rate limit for the whole host, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte (default 104857600.000000)
  -u, --url string                   URL of user requested downloading file(only HTTP/HTTPs supported)
      --verbose                      enable verbose mode, all debug log will be display

```

# dfget daemon

## Log configuration

set environment variable DF_ACTIVE_PROFILE=local if you want to print logs to Terminal

### Options

```
      --advertise-ip string       the ip report to scheduler, normal same with listen ip (default "10.15.232.63")
      --alivetime duration       alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit (default 5m0s)
      --data string               local directory which stores temporary files for p2p uploading
      --download-rate ratelimit   download rate limit for other peers and back source (default 104857600.000000)
      --expiretime duration      caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted (default 3m0s)
      --gc-interval duration      gc interval (default 1m0s)
      --grpc-port int             the listen address for grpc with other peers (default 65000)
      --grpc-port-end int         the listen address for grpc with other peers (default 65000)
      --grpc-unix-listen string   the local unix domain socket listen address for grpc with dfget (default "/tmp/dfdamon.sock")
  -h, --help                      help for daemon
      --home string               the work home directory of dfget daemon
      --idc string                peer idc for scheduler
      --keep-storage              keep storage after daemon exit
      --ip string                 IP address that server will listen on
      --location string           peer location for scheduler
      --lock string               dfdaemon lock file location (default "/tmp/dfdaemon.lock")
      --net-topology string       peer net topology for scheduler
      --pid string                dfdaemon pid file location (default "/tmp/dfdaemon.pid")
      --proxy-port int            the address that daemon will listen on for proxy rest (default 65001)
      --proxy-port-end int        the address that daemon will listen on for proxy rest (default 65001)
  -s, --schedulers schedulers     schedulers
      --security-domain string    peer security domain for scheduler
      --upload-port int           the address that daemon will listen on for peer upload (default 65002)
      --upload-port-end int       the address that daemon will listen on for peer upload (default 65002)
      --upload-rate ratelimit     upload rate limit for other peers (default 104857600.000000)
      --verbose                   print verbose log and enable golang debug info
```
