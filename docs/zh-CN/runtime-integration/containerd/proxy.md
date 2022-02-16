# 使用 dfget daemon 作为 containerd 的 http 代理

目前 containerd 的 `ctr` 命令不支持带有
registry-mirrors 的私有注册表，为此我们需要为 containerd 使用 HTTP 代理。

## 快速开始

### 第一步：为 http 代理生成 CA 证书

生成一个 CA 证书私钥。

```bash
openssl genrsa -out ca.key 2048
```

打开 openssl 配置文件 `openssl.conf`。设置 `basicConstraints` 为 true，然后您就能修改这些值。

```text
[ req ]
#default_bits = 2048
#default_md = sha256
#default_keyfile = privkey.pem
distinguished_name = req_distinguished_name
attributes = req_attributes
extensions               = v3_ca
req_extensions           = v3_ca

[ req_distinguished_name ]
countryName = Country Name (2 letter code)
countryName_min = 2
countryName_max = 2
stateOrProvinceName = State or Province Name (full name)
localityName = Locality Name (eg, city)
0.organizationName = Organization Name (eg, company)
organizationalUnitName = Organizational Unit Name (eg, section)
commonName = Common Name (eg, fully qualified host name)
commonName_max = 64
emailAddress = Email Address
emailAddress_max = 64

[ req_attributes ]
challengePassword = A challenge password
challengePassword_min = 4
challengePassword_max = 20

[ v3_ca ]
basicConstraints         = CA:TRUE
```

生成 CA 证书。

```bash
openssl req -new -key ca.key -nodes -out ca.csr -config openssl.conf
openssl x509 -req -days 36500 -extfile openssl.conf \
      -extensions v3_ca -in ca.csr -signkey ca.key -out ca.crt
```

### 第二步：配置 dfget daemon

为了将 dfget daemon 作为 http 代理使用，首先你需要在 `/etc/dragonfly/dfget.yaml` 中增加一条代理规则，
它将会代理 `your.private.registry` 对镜像层的请求：

```yaml
proxy:
  security:
    insecure: true
  tcpListen:
    listen: 0.0.0.0
    port: 65001
  proxies:
    - regx: blobs/sha256.*
  hijackHTTPS:
    # CA certificate's path used to hijack https requests
    cert: ca.crt
    key: ca.key
    hosts:
      - regx: your.private.registry
```

### 第三步：配置 containerd

在 `/etc/systemd/system/containerd.service.d/http-proxy.conf`
设置 dfdaemon 为 docker daemon 的 `HTTP_PROXY` 和 `HTTPS_PROXY` 代理：

```toml
[Service]
Environment="HTTP_PROXY=http://127.0.0.1:65001"
Environment="HTTPS_PROXY=http://127.0.0.1:65001"
```

### 第四步：使用代理拉取镜像

完成以上的步骤后，我们可以尝试验证 Dragonfly 是否像我们预期的一样正常工作。

您可以像往常一样拉取镜像，比如：

```bash
ctr image pull your.private.registry/namespace/image:latest
```

## 自定义配置项

### 使用自签名证书注册

如果您使用一个自签名证书进行注册，你可以用以下配置来忽略证书错误：

```yaml
proxy:
  security:
    insecure: true
  tcpListen:
    listen: 0.0.0.0
    port: 65001
  proxies:
    - regx: blobs/sha256.*
  hijackHTTPS:
    # CA certificate's path used to hijack https requests
    cert: ca.crt
    key: ca.key
    hosts:
      - regx: your.private.registry
        insecure: true
```

也可以使用以下配置提供一个证书：

```yaml
proxy:
  security:
    insecure: true
  tcpListen:
    listen: 0.0.0.0
    port: 65001
  proxies:
    - regx: blobs/sha256.*
  hijackHTTPS:
    # CA certificate's path used to hijack https requests
    cert: ca.crt
    key: ca.key
    hosts:
      - regx: your.private.registry
        certs: ['server.crt']
```

您能使用以下命令获取您服务器的证书：

```bash
openssl x509 -in <(openssl s_client -showcerts \
    -servername your.domain.com -connect your.domain.com:443 -prexit 2>/dev/null)
```
