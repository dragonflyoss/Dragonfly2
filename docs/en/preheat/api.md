# API

Use preheat apis for preheating. First create a POST request for preheating, you can refer to [create preheat api document](../api-reference/api-reference.md#create-preheat)

If the `scheduler_cluster_ids` does not exist, it means to preheat all scheduler clusters.

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

If the output of command above has content like

```bash
{ "id": 1 "task_id": "group_4d1ea00e-740f-4dbf-a47e-dbdc08eb33e1", "type": "preheat", "status": "PENDING", "args": { "filter": "", "headers": null, "type": "image", "url": "https://registry-1.docker.io/v2/library/redis/manifests/latest" }}
```

Polling the preheating status with id. if status is `SUCCESS`, preheating is successful, you can refer to [get preheat api document](../api-reference/api-reference.md#get-preheat)

```bash
curl --request GET 'http://dragonfly-manager:8080/api/v1/jobs/1'
```

If the status is `SUCCESS`, the preheating is successful.

```bash
{ "id": 1 "task_id": "group_4d1ea00e-740f-4dbf-a47e-dbdc08eb33e1", "type": "preheat", "status": "SUCCESS", "args": { "filter": "", "headers": null, "type": "image", "url": "https://registry-1.docker.io/v2/library/redis/manifests/latest" }}
```
