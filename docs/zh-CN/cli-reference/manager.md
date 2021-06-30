## Scheduler

Manager 是一个常驻后台运行的进程，它在蜻蜓中扮演各个子系统集群大脑的角色， 用于管理各个系统模块依赖的动态配置，以及提供心跳保活、监控大盘和产品化的功能。

### 用法
```
go run cmd/manager/main.go [Option]
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