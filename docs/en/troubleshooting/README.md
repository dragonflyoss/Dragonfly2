# Troubleshooting Guide

## Download slowly than without Dragonfly

1. Confirm limit rate in [dfget.yaml](https://github.com/dragonflyoss/Dragonfly2/blob/main/docs/en/deployment/configuration/dfget.yaml#L65)

```yaml
download:
  # total download limit per second
  totalRateLimit: 200Mi
  # per peer task download limit per second
  perPeerRateLimit: 100Mi # default is 20Mi, this default is in consideration of extreme environments.
upload:
  # upload limit per second
  rateLimit: 100Mi
```

2. Confirm source connection speed in CDN and dfdaemon

## 500 Internal Server Error

1. Check error logs in /var/log/dragonfly/daemon/core.log
2. Check source connectivity(dns error or certificate error)

Example:
```shell
curl https://example.harbor.local/
```

When curl says error, please check the details in output.
