# Simple Stress Testing Tools for Dragonfly

## Daemon Proxy

### Build and Run

1. Build tool:

    ```shell
    go build -o bin/stress test/stress/main.go
    ```

2. Run stress:

    ```shell
    bin/stress -connections 100 -duration 1s -proxy http://127.0.0.1:65001 \
        --url http://localhost/misc/d7y-test/blobs/sha256/128K
    ```

    Example output:

    ```text
    Latency
      avg  17.286522ms
      min  617.801µs
      max  84.201941ms
    Latency Distribution
      50% 11.39049ms
      75% 18.308966ms
      90% 49.052485ms
      95% 55.886513ms
      99% 65.013042ms
    HTTP codes
      200  5849
    Throughput 731.1MB
    Request 5849/s
    ```

### CLI Reference

```text
Usage of ./stress:
  -connections int
     concurrency count of connections (default 100)
  -duration duration
     testing duration (default 1m40s)
  -output string
     all request statistics (default "/tmp/statistics.txt")
  -proxy string
     target proxy for downloading, example: http://127.0.0.1:65001
  -url string
     target url for stress testing, example: http://localhost
```
