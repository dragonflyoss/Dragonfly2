# Scheduler

Scheduler 是一个常驻后台运行的进程，用于接收和管理客户端的下载任务，
通知 CDN 进行回源， 在下载过程中生成维护 P2P 网络，给客户端推送适合的下载节点

## 用法

```text
scheduler [flags]
scheduler [command]
```

## 可用子命令

```text
doc         生成帮助文档
help        命令的帮助文档
version     查看 scheduler 当前版本
```

## 可选参数

```text
--config string         配置文件地址 (默认值为/Users/${USER_HOME}/.dragonfly/config/scheduler.yaml)，
                        也可以使用环境变量 scheduler_config 进行配置
--console               logger 日志是否输出记录到标准输出
-h, --help              显示 scheduler 命令帮助信息
--jaeger string         jaeger 收集器 endpoint url, 例如: http://localhost:14250/api/traces
--pprof-port int        pprof 监听的端口，为 0 时使用随机端口（默认值为 -1）
--service-name string   tracer分布式追踪日志中的服务名称 (默认 "dragonfly-scheduler")
--verbose               是否使用debug级别的日志输出
```
