## CDN

CDN is a long-running process which caches downloaded data from source to avoid downloading the same files from source repeatedly

## 用法
```
go run cmd/cdnsystem/main.go [Option]
```
## Log configuration
set environment variable DF_ACTIVE_PROFILE=local if you want to print logs to Terminal

## Runtime metrics monitoring
```
go run cmd/cdnsystem/main.go --profiler
```
### 可选参数

```
      --config string    the path of configuration file with yaml extension name, default is /Users/${USER_HOME}/.dragonfly/config/cdn.yaml, it can also be 
      set by env var:CDN_CONFIG，The settings and uses of each configuration item can refer to cdn.yaml in config directory
      --console          whether logger output records to the stdout
  -h, --help             help for cdn
      --jaeger string    jaeger endpoint url, like: http://localhost:14250/api/traces
      --pprof-port int   listen port for pprof, 0 represents random port (default -1)
      --verbose          whether logger use debug level
```

### 二进制命令使用示例

scheduler --config your-config-path/scheduler.yaml

### 配置文件说明

```
server:
  port: 8001                        rpc 端口

scheduler:

worker:
  worker-num: 5                     工作线程数
  worker-job-pool-size: 10000       工作队列长度
  sender-num: 10                    发送消息线程数
  sender-job-pool-size: 10000       发送消息队列长度

cdn:
  list:                             CDN列表
    -
      - cdn-name : "cdn"            CDN服务器的HostName
        ip: "127.0.0.1"             CDN服务器的IP地址
        rpcPort: 8003               CDN的RPC端口
        download-port: 8002         CDN的下载端口
```
