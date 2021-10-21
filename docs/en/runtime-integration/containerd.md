# Use dfget daemon as HTTP proxy for containerd

Currently, `ctr` command of containerd doesn't support private registries with `registry-mirrors`,
in order to do so, we need to use HTTP proxy for containerd.

## Quick Start

### Step 1: Generate CA certificate for HTTP proxy

Generate a CA certificate private key.

```bash
openssl genrsa -out ca.key 2048
```

Open openssl config file `openssl.conf`. Note set `basicConstraints` to true, that you can modify the values.

```text
[ req ]
#default_bits		= 2048
#default_md		= sha256
#default_keyfile 	= privkey.pem
distinguished_name	= req_distinguished_name
attributes		= req_attributes
extensions               = v3_ca
req_extensions           = v3_ca

[ req_distinguished_name ]
countryName			= Country Name (2 letter code)
countryName_min			= 2
countryName_max			= 2
stateOrProvinceName		= State or Province Name (full name)
localityName			= Locality Name (eg, city)
0.organizationName		= Organization Name (eg, company)
organizationalUnitName		= Organizational Unit Name (eg, section)
commonName			= Common Name (eg, fully qualified host name)
commonName_max			= 64
emailAddress			= Email Address
emailAddress_max		= 64

[ req_attributes ]
challengePassword		= A challenge password
challengePassword_min		= 4
challengePassword_max		= 20

[ v3_ca ]
basicConstraints         = CA:TRUE
```

Generate the CA certificate.

```bash
openssl req -new -key ca.key -nodes -out ca.csr -config openssl.conf
openssl x509 -req -days 36500 -extfile openssl.conf -extensions v3_ca -in ca.csr -signkey ca.key -out ca.crt
```

### Step 2: Configure dfget daemon

To use dfget daemon as HTTP proxy, first you need to append a proxy rule in
`/etc/dragonfly/dfget.yaml`, This will proxy `your.private.registry`'s requests for image layers:

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

### Step 3: Configure containerd

Set dfget damone as `HTTP_PROXY` and `HTTPS_PROXY` for containerd in
`/etc/systemd/system/containerd.service.d/http-proxy.conf`:

```
[Service]
Environment="HTTP_PROXY=http://127.0.0.1:65001"
Environment="HTTPS_PROXY=http://127.0.0.1:65001"
```

### Step 4: Pull images with proxy

Through the above steps, we can start to validate if Dragonfly works as expected.

And you can pull the image as usual, for example:

```bash
ctr image pull your.private.registry/namespace/image:latest
```

## Custom assets

### Registry uses a self-signed certificate

If your registry uses a self-signed certificate, you can either choose to
ignore the certificate error with:

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

Or provide a certificate with:

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
        certs: ["server.crt"]
```

You can get the certificate of your server with:

```
openssl x509 -in <(openssl s_client -showcerts -servername xxx -connect xxx:443 -prexit 2>/dev/null)
```

# Use dfget daemon as registry mirror for Containerd with CRI support

From v1.1.0, Containerd supports registry mirrors, we can configure Containerd via this feature for HA.

## Quick Start

### Step 1: Configure dfget daemon

To use dfget daemon as registry mirror, first you need to ensure configuration in `/etc/dragonfly/dfget.yaml`:

```yaml
proxy:
  security:
    insecure: true
  tcpListen:
    listen: 0.0.0.0
    port: 65001
  registryMirror:
    # multiple registries support, if only mirror single registry, disable this
    dynamic: true
    url: https://index.docker.io
  proxies:
    - regx: blobs/sha256.*
```

Run dfget daemon

```shell
dfget daemon
```

## Step 2: Configure Containerd

### Option 1: Single Registry

Enable mirrors in Containerd registries configuration in
`/etc/containerd/config.toml`:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2

[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
  endpoint = ["http://127.0.0.1:65001","https://registry-1.docker.io"]
```

In this config, there is two mirror endpoints for "docker.io", Containerd will pull images with `http://127.0.0.1:65001` first.
If `http://127.0.0.1:65001` is not available, the default `https://registry-1.docker.io` will be used for HA.

> More details about Containerd configuration: https://github.com/containerd/containerd/blob/v1.5.2/docs/cri/registry.md#configure-registry-endpoint

> Containerd has deprecated the above config from v1.4.0, new format for reference: https://github.com/containerd/containerd/blob/v1.5.2/docs/cri/config.md#registry-configuration

### Option 2: Multiple Registries

This option only supports Containerd 1.5.0+.

#### 1. Enable Containerd Registries Config Path

Enable mirrors in Containerd registries config path in
`/etc/containerd/config.toml`:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2

[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"
```

#### 2. Generate Per Registry hosts.toml

##### Option 1: Generate hosts.toml manually

Path: `/etc/containerd/certs.d/example.com/hosts.toml`

Replace `example.com` according the different registry domains.

Content:

```toml
server = "https://example.com"

[host."http://127.0.0.1:65001"]
  capabilities = ["pull", "resolve"]
  [host."http://127.0.0.1:65001".header]
    X-Dragonfly-Registry = ["https://example.com"]
```

##### Option 2: Generate hosts.toml automatically

You can also generate hosts.toml with https://github.com/dragonflyoss/Dragonfly2/blob/main/hack/gen-containerd-hosts.sh

```shell
bash gen-containerd-hosts.sh example.com
```

> More details about registry configuration: https://github.com/containerd/containerd/blob/main/docs/hosts.md#registry-configuration---examples

## Step 3: Restart Containerd Daemon

```
systemctl restart containerd
```

## Step 4: Pull Image

You can pull image like this:

```
crictl pull docker.io/library/busybox
```

## Step 5: Validate Dragonfly

You can execute the following command to check if the busybox image is distributed via Dragonfly.

```bash
grep 'register peer task result' /var/log/dragonfly/daemon/*.log
```

If the output of command above has content like

```
{"level":"info","ts":"2021-02-23 20:03:20.306","caller":"client/client.go:83","msg":"register peer task result:true[200] for taskId:adf62a86f001e17037eedeaaba3393f3519b80ce,peerIp:10.15.233.91,securityDomain:,idc:,scheduler:127.0.0.1:8002","peerId":"10.15.233.91-65000-43096-1614081800301788000","errMsg":null}
```
