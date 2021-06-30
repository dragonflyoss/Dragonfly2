## Scheduler

Scheduler 是一个常驻后台运行的进程，用于接收和管理客户端的下载任务，通知CDN进行回源， 在下载过程中生成维护P2P网络，给客户端推送适合的下载节点

### 用法
```
go run cmd/scheduler/main.go [Option]
```

### 可选参数

```
      --config string              the path of scheduler's configuration file (default "conf/scheduler.yaml")
  -h, --help                       help for scheduler
      --port int                   port is the port that scheduler server listens on (default 8002)
      --sender-job-pool-size int   sender-job-pool-size is used for scheduler and do not change it (default 10000)
      --sender-num int             sender-num is used for scheduler and do not change it (default 50)
      --worker-job-pool-size int   worker-job-pool-size is used for scheduler and do not change it (default 10000)
      --worker-num int             worker-num is used for scheduler and do not change it (default 12)
```