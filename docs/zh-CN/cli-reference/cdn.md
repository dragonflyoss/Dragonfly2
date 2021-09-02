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
      --config string    the path of configuration file with yaml extension name, default is /Users/${USER_HOME}/.dragonfly/config/cdn.yaml, it can also be 
      set by env var:CDN_CONFIG，The settings and uses of each configuration item can refer to cdn.yaml in config directory
      --console          whether logger output records to the stdout
  -h, --help             help for cdn
      --jaeger string    jaeger endpoint url, like: http://localhost:14250/api/traces
      --pprof-port int   listen port for pprof, 0 represents random port (default -1)
      --verbose          whether logger use debug level
```