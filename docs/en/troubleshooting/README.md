# Troubleshooting Guide

## Change log level

Send `SIGUSR1` signal to dragonfly process to change log level

```shell
kill -s SIGUSR1 <pid of dfdaemon, scheduler, cdn, or manager>
```

stdout:

```text
change log level to debug
change log level to fatal
change log level to panic
change log level to dpanic
change log level to error
change log level to warn
change log level to info
```

> The change log level event will print in stdout and `core.log` file, but if the level is greater than `info`, stdout only.

## Download slowly than without Dragonfly

1. Confirm limit rate in [dfget.yaml](https://github.com/dragonflyoss/Dragonfly2/blob/main/docs/en/deployment/configuration/dfget.yaml#L65)

    ```yaml
    download:
      # total download limit per second
      totalRateLimit: 200Mi
      # per peer task download limit per second
      # default is 20Mi, this default is in consideration of extreme environments
      perPeerRateLimit: 100Mi 
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
