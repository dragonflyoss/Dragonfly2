# API

用户使用 api 进行预热。首先发送 POST 请求创建预热任务，具体 api 可以参考文档 [create job api document](../api-reference/api-reference.md#create-job)。

如果 `scheduler_cluster_ids` 不存在，表示对所有 scheduler cluster 进行预热。

```bash
curl --location --request POST 'http://dragonfly-manager:8080/api/v1/jobs' \
--header 'Content-Type: application/json' \
--data-raw '{
    "type": "preheat",
    "args": {
        "type": "image",
        "url": "https://registry-1.docker.io/v2/library/redis/manifests/latest"
    }
}'
```

命令行日志返回预热任务 ID。

```bash
{ "id": 1 "task_id": "group_4d1ea00e-740f-4dbf-a47e-dbdc08eb33e1", "type": "preheat", "status": "PENDING", "args": { "filter": "", "headers": null, "type": "image", "url": "https://registry-1.docker.io/v2/library/redis/manifests/latest" }}
```

使用预热任务 ID 轮训查询任务是否成功，具体 api 可以参考文档 [get job api document](../api-reference/api-reference.md#get-job)。

```bash
curl --request GET 'http://dragonfly-manager:8080/api/v1/jobs/1'
```

如果返回预热任务状态为 `SUCCESS`，表示预热成功。

```bash
{ "id": 1 "task_id": "group_4d1ea00e-740f-4dbf-a47e-dbdc08eb33e1", "type": "preheat", "status": "SUCCESS", "args": { "filter": "", "headers": null, "type": "image", "url": "https://registry-1.docker.io/v2/library/redis/manifests/latest" }}
```