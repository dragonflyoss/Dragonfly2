# console

用户使用 api 进行预热。 首先访问 Dragonfly Manager Console (http://dragonfly-manager:8080/service/task) 创建预热任务。

如果不选择 `Scheduler Cluster` 则是对所有 Scheduler 进行预热。

![add-preheat-job](../../en/images/manager-console/add-preheat-job.png)


查看预热任务列表

![preheat-list](../../en/images/manager-console/preheat-list.png)

点击 `Description` 查看预热详情

![preheat-list](../../en/images/manager-console/preheat-job-detail.png)


### FAQ

#### 创建预热任务报错

如果创建预热任务报错，接口返回500，页面提示 `http error`，需要排查 redis 是否具有写权限。
Dragonfly Manager 日志报错类似

```bash
2022-02-15T08:30:01.123Z ERROR job/preheat.go:159 create preheat group job failedREADONLY You  can't write against a read only replica.
d7y.io/dragonfly/v2/manager/job.(*preheat).createGroupJob
/go/src/d7y.io/dragonfly/v2/manager/job/preheat.go:159
d7y.io/dragonfly/v2/manager/job.(*preheat).CreatePreheat
/go/src/d7y.io/dragonfly/v2/manager/job/preheat.go:128
d7y.io/dragonfly/v2/manager/service.(*rest).CreatePreheatJob
/go/src/d7y.io/dragonfly/v2/manager/service/job.go:71
d7y.io/dragonfly/v2/manager/handlers.(*Handlers).CreateJob
/go/src/d7y.io/dragonfly/v2/manager/handlers/job.go:40
github.com/gin-gonic/gin.(*Context).Next
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/context.go:168
d7y.io/dragonfly/v2/manager/middlewares.Error.func1
/go/src/d7y.io/dragonfly/v2/manager/middlewares/error.go:42
github.com/gin-gonic/gin.(*Context).Next
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/context.go:168
github.com/gin-gonic/gin.CustomRecoveryWithWriter.func1
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/recovery.go:99
github.com/gin-gonic/gin.(*Context).Next
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/context.go:168
github.com/gin-gonic/gin.LoggerWithConfig.func1
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/logger.go:241
github.com/gin-gonic/gin.(*Context).Next
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/context.go:168
github.com/mcuadros/go-gin-prometheus.(*Prometheus).HandlerFunc.func1
/go/pkg/mod/github.com/mcuadros/go-gin-prometheus@v0.1.0/middleware.go:364
github.com/gin-gonic/gin.(*Context).Next
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/context.go:168
github.com/gin-gonic/gin.(*Engine).handleHTTPRequest
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/gin.go:555
github.com/gin-gonic/gin.(*Engine).ServeHTTP
/go/pkg/mod/github.com/gin-gonic/gin@v1.7.7/gin.go:511
net/http.serverHandler.ServeHTTP
/usr/local/go/src/net/http/server.go:2878
net/http.(*conn).serve
/usr/local/go/src/net/http/server.go:1929
```