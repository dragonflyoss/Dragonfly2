# Prometheus Metrics

This doc contains all the metrics that Dragonfly components currently support.
Now we support metrics for Dfdaemon, Scheduler and CDN.
The metrics path is fixed to /metrics. The following metrics are exported.

## Dfdaemon

<!-- markdownlint-disable -->

| Name                                                     | Labels | Type    | Description                                           |
| :------------------------------------------------------- | :----- | :------ | :---------------------------------------------------- |
| dragonfly_dfdaemon_proxy_request_total                   | method | counter | Counter of the total proxy request.                   |
| dragonfly_dfdaemon_proxy_request_via_dragonfly_total     |        | counter | Counter of the total proxy request via Dragonfly.     |
| dragonfly_dfdaemon_proxy_request_not_via_dragonfly_total |        | counter | Counter of the total proxy request not via Dragonfly. |
| dragonfly_dfdaemon_proxy_request_running_total           | method | counter | Current running count of proxy request.               |
| dragonfly_dfdaemon_proxy_request_bytes_total             | method | counter | Counter of the total byte of all proxy request.       |
| dragonfly_dfdaemon_peer_task_total                       |        | counter | Counter of the total peer tasks.                      |
| dragonfly_dfdaemon_peer_task_failed_total                |        | counter | Counter of the total failed peer tasks.               |
| dragonfly_dfdaemon_piece_task_total                      |        | counter | Counter of the total failed piece tasks.              |
| dragonfly_dfdaemon_piece_task_failed_total               |        | counter | Dragonfly dfget tasks.                                |
| dragonfly_dfdaemon_file_task_total                       |        | counter | Counter of the total file tasks.                      |
| dragonfly_dfdaemon_stream_task_total                     |        | counter | Counter of the total stream tasks.                    |
| dragonfly_dfdaemon_peer_task_cache_hit_total             |        | counter | Counter of the total cache hit peer tasks.            |

<!-- markdownlint-restore -->

## Scheduler

<!-- markdownlint-disable -->

| Name                                                         | Labels                                     | Type      | Description                                                |
| :----------------------------------------------------------- | :----------------------------------------- | :-------- | :--------------------------------------------------------- |
| dragonfly_scheduler_register_peer_task_total                 |                                            | counter   | Counter of the number of the register peer task.           |
| dragonfly_scheduler_register_peer_task_failure_total         |                                            | counter   | Counter of the number of failed of the register peer task. |
| dragonfly_scheduler_download_total                           |                                            | counter   | Counter of the number of the downloading.                  |
| dragonfly_scheduler_download_failure_total                   |                                            | counter   | Counter of the number of failed of the downloading.        |
| dragonfly_scheduler_p2p_traffic                              |                                            | counter   | Counter of the number of p2p traffic.                      |
| dragonfly_scheduler_peer_host_traffic                        | traffic_type, peer_host_uuid, peer_host_ip | counter   | Counter of the number of per peer host traffic.            |
| dragonfly_scheduler_peer_task_total                          | type                                       | counter   | Counter of the number of peer task.                        |
| dragonfly_scheduler_peer_task_download_duration_milliseconds |                                            | histogram | Histogram of the time each peer task downloading.          |
| dragonfly_scheduler_concurrent_schedule_total                |                                            | gauge     | Gauge of the number of concurrent of the scheduling.       |

<!-- markdownlint-restore -->

## CDN

<!-- markdownlint-disable -->

| Name                                    | Labels | Type    | Description                                            |
| :-------------------------------------- | :----- | :------ | :----------------------------------------------------- |
| dragonfly_cdn_download_total            |        | counter | Counter of the number of the downloading.              |
| dragonfly_cdn_download_failure_total    |        | counter | Counter of the number of failed of the downloading.    |
| dragonfly_cdn_download_traffic          |        | counter | Counter of the number of download traffic.             |
| dragonfly_cdn_concurrent_download_total |        | gauge   | Gauger of the number of concurrent of the downloading. |

<!-- markdownlint-restore -->
