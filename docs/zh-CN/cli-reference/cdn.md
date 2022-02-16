# CDN

CDN 是一个长时间运行的服务进程，它缓存从源下载的数据，以避免重复从源下载相同的文件

## 用法

```text
cdn [flags]
cdn [command]
```

## 可用子命令

```text
doc         生成帮助文档
help        命令的帮助文档
version     查看 cdn 当前版本
```

## 可选参数

<!-- markdownlint-disable -->

```text
      --config string         配置文件地址 (默认值为/Users/${USER_HOME}/.dragonfly/config/cdn.yaml)，也可以使用环境变量 cdn_config 进行配置
      --console               logger 日志是否输出记录到标准输出
  -h, --help                  显示 cdn 命令帮助信息
      --jaeger string         jaeger 收集器 endpoint url, 例如: http://localhost:14250/api/traces
      --pprof-port int        pprof 监听的端口，为 0 时使用随机端口（默认值为 -1）
      --service-name string   tracer分布式追踪日志中的服务名称 (默认 "dragonfly-scheduler")
      --verbose               是否使用debug级别的日志输出
```

<!-- markdownlint-restore -->
