# Preheat

P2P provides two types of preheating: `image` and `file`. Users can be preheat in the `console` or directly call `preheat api` for preheating

## Console

TODO

## API

Use preheat apis for preheating. First create a POST request for preheating, you can refer to [create preheat api document](../../api/api.md#create-preheat)

If the `scheduler_cluster_id` does not exist, it means to preheat all scheduler clusters.

```bash
curl --request POST 'http://dragonfly-manager:8080/api/v1/preheats' \
--header 'Content-Type: application/json' \
--data-raw '{
    "type": "image",
    "url": "https://registry-1.docker.io/v2/library/busybox/manifests/latest",
    "scheduler_cluster_id": 1
}'
```

If the output of command above has content like

```bash
{"id":"group_28439e0b-d4c3-43bf-945e-482b54c49dc5","status":"PENDING","create_at":"2021-10-09T11:54:50.6182794Z"}
```

Polling the preheating status with id. if status is `SUCCESS`, preheating is successful, you can refer to [get preheat api document](../../api/api.md#get-preheat)

```bash
curl --request GET 'http://manager-domain:8080/api/v1/preheats/group_28439e0b-d4c3-43bf-945e-482b54c49dc5'
```

If the status is `SUCCESS`, the preheating is successful.

```bash
{"id":"group_28439e0b-d4c3-43bf-945e-482b54c49dc5","status":"SUCCESS","create_at":"2021-10-09T11:54:50.5712334Z"}
```
