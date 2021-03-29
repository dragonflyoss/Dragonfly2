## 调度器 

调度器生成并维护下载过程中的P2P网络

### 说明

调度器是一个常驻后台运行的进程，用于接收和管理客户端的下载任务，通知CDN进行回源， 在下载过程中生成维护P2P网络，给客户端推送适合的下载节点

### 用法
```
scheduler [flags]
```

### 可选参数

```
      --config string              the path of scheduler's configuration file (default "conf/scheduler.yml")
  -h, --help                       help for scheduler
      --port int                   port is the port that scheduler server listens on (default 8002)
      --sender-job-pool-size int   sender-job-pool-size is used for scheduler and do not change it (default 10000)
      --sender-num int             sender-num is used for scheduler and do not change it (default 50)
      --worker-job-pool-size int   worker-job-pool-size is used for scheduler and do not change it (default 10000)
      --worker-num int             worker-num is used for scheduler and do not change it (default 12)
```

### 使用示例

scheduler --config your-config-path/scheduler.yml

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
