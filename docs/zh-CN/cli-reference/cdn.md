## CDN

CDN 是一个长时间运行的服务进程，它缓存从源下载的数据，以避免重复从源下载相同的文件

## 用法
```
go run cmd/cdnsystem/main.go [Option]
```
## 输出日志配置
如果您想要将日志输出到命令终端，请使用 --console 将环境变量 console 设置为 true。

## Runtime metrics monitoring
```
go run cmd/cdnsystem/main.go --profiler
```
### 可选参数

```
      --config string         配置文件地址，默认值为/Users/{username}/.dragonfly/config/cdn.yaml。也能使用环境变量 CDN_CONFIG 进行配置。
      --console               是否在控制台中显示日志信息
  -h, --help                  显示 cdn 的帮助信息
      --jaeger string         配置 jaeger 地址 url，例如 http://localhost:14250/api/traces
      --pprof-port int        pprof 监听的端口，为 0 时使用随机端口（默认值为 -1）
      --service-name string   在 tracer 中使用的服务名（默认值为"dragonfly-cdn"）
      --verbose               是否开启调试级别的日志打印

```