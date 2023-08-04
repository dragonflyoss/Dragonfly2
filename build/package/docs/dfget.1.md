% DFGET(1) Version v2.1.0 | Frivolous "Dfget" Documentation

# NAME

**dfget** — client of Dragonfly used to download and upload files

# SYNOPSIS

dfget is the client of Dragonfly which takes a role of peer in a P2P network. When user triggers a file downloading
task, dfget will download the pieces of file from other peers. Meanwhile, it will act as an uploader to support other
peers to download pieces from it if it owns them. In addition, dfget has the abilities to provide more advanced
functionality, such as network bandwidth limit, transmission encryption and so on.

## OPTIONS

```shell
      --accept-regex string   Recursively download only. Specify a regular expression to accept the complete URL. In this case, you have to enclose the pattern into quotes to prevent your shell from expanding it
      --config string         the path of configuration file with yaml extension name, it can also be set by env var: DFGET_CONFIG
      --console               whether logger output records to the stdout
      --daemon-sock string    Download socket path of daemon. In linux, default value is /var/run/dfdaemon.sock, in macos(just for testing), default value is /tmp/dfdaemon.sock
      --digest string         Check the integrity of the downloaded file with digest, in format of md5:xxx or sha256:yyy
      --disable-back-source   Disable downloading directly from source when the daemon fails to download file
      --filter string         Filter the query parameters of the url, P2P overlay is the same one if the filtered url is same, in format of key&sign, which will filter 'key' and 'sign' query parameters
  -H, --header strings        url header, eg: --header='Accept: *' --header='Host: abc'
  -h, --help                  help for dfget
      --jaeger string         jaeger endpoint url, like: http://localhost:14250/api/traces
      --level uint            Recursively download only. Set the maximum number of subdirectories that dfget will recurse into. Set to 0 for no limit (default 5)
  -l, --list                  Recursively download only. List all urls instead of downloading them.
      --logdir string         Dfget log directory
      --original-offset       Range request only. Download ranged data into target file with original offset. Daemon will make a hardlink to target file. Client can download many ranged data into one file for same url. When enabled, back source in client will be disabled
  -O, --output string         Destination path which is used to store the downloaded file, it must be a full path
  -P, --priority string       Scheduler will schedule task according to priority
      --pprof-port int        listen port for pprof, 0 represents random port (default -1)
      --range string          Download range. Like: 0-9, stands download 10 bytes from 0 -9, [0:9] in real url
      --ratelimit string      The downloading network bandwidth limit per second in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will be parsed as Byte, 0 is infinite (default "100.0MB")
  -r, --recursive             Recursively download all resources in target url, the target source client must support list action
      --reject-regex string   Recursively download only. Specify a regular expression to reject the complete URL. In this case, you have to enclose the pattern into quotes to prevent your shell from expanding it
      --service-name string   name of the service for tracer (default "dragonfly-dfget")
  -b, --show-progress         Show progress bar, it conflicts with --console
      --tag string            Different tags for the same url will be divided into different P2P overlay, it conflicts with --digest
      --timeout duration      Timeout for the downloading task, 0 is infinite
  -u, --url string            Download one file from the url, equivalent to the command's first position argument
      --verbose               whether logger use debug level
      --workhome string       Dfget working directory
```

# BUGS

See GitHub Issues: <https://github.com/dragonflyoss/Dragonfly2/issues>
