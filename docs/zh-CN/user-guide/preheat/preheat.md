# 预热

P2P 加速可预热两种类型数据 `image` 和 `file`, 用户可以在控制台操作或者直接调用 api 进行预热。

## Console

TODO

## API

用户使用 api 进行预热。首先发送 POST 请求创建预热任务，具体 api 可以参考文档 [create preheat api document](../../api/api.md#create-preheat)。

```bash
curl --request POST 'http://dragonfly-manager:8080/api/v1/preheats' \
--header 'Content-Type: application/json' \
--data-raw '{
    "type": "image",
    "url": "https://registry-1.docker.io/v2/library/busybox/manifests/latest",
    "scheduler_cluster_id": 1
}'
```

命令行日志返回预热任务 ID。

```bash
{"id":"group_28439e0b-d4c3-43bf-945e-482b54c49dc5","status":"PENDING","create_at":"2021-10-09T11:54:50.6182794Z"}
```

使用预热任务 ID 轮训查询任务是否成功，具体 api 可以参考文档 [get preheat api document](../../api/api.md#get-preheat)。


```bash
curl --request GET 'http://manager-domain:8080/api/v1/preheats/group_28439e0b-d4c3-43bf-945e-482b54c49dc5'
```

如果返回预热任务状态为 `SUCCESS`，表示预热成功。

```bash
{"id":"group_28439e0b-d4c3-43bf-945e-482b54c49dc5","status":"SUCCESS","create_at":"2021-10-09T11:54:50.5712334Z"}
```
