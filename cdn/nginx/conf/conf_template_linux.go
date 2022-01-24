//go:build linux
// +build linux

/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package conf

const Template = `
worker_rlimit_nofile 100000;

events {
  use                     epoll;
  worker_connections      20480;
  # multi_accept on;
}

http {
  include                 /etc/nginx/mime.types;
  default_type            application/octet-stream;

  sendfile                on;
  tcp_nopush              on;
  tcp_nodelay             on;

  server_tokens           off;
  
  keepalive_timeout       5;
  
  client_header_timeout   1m;
  send_timeout            1m;
  client_max_body_size    3m;
  
  types_hash_max_size     2048;
   
  index                   index.html index.htm;
  access_log              off;
  log_not_found           off;

  ##
  # Gzip Settings
  ##

  gzip                    on;
  gzip_http_version       1.0;
  gzip_comp_level         6;
  gzip_min_length         1024;
  gzip_proxied            any;
  gzip_vary               on;
  gzip_disable            msie6;
  gzip_buffers            96 8k;
  gzip_types              text/xml text/plain text/css application/javascript application/x-javascript application/rss+xml application/json;


  ##
  # Proxy Settings
  ##

  proxy_set_header        X-Forwarded-Proto $http_x_forwarded_proto;
  proxy_set_header        X-Original-URI    $request_uri;
  proxy_set_header        Host $host;
  proxy_set_header        X-Real-IP $remote_addr;
  proxy_set_header        Web-Server-Type nginx;
  proxy_set_header        WL-Proxy-Client-IP $remote_addr;
  proxy_set_header        X-Forwarded-For $proxy_add_x_forwarded_for;
  proxy_redirect          off;
  proxy_buffers           128 8k;
  proxy_intercept_errors  on;

  server {
	listen {{.port}};
	{{if .ssl_enabled}}
    rewrite ^(.*)$ https://$host$1 permanent; 
	{{else}}
    access_log {{.access_log_path}};
    error_log {{.error_log_path}};

    gzip on;
    gzip_types text/plain test/csv application/json;
	location /download {
       alias {{.repo}};
    }
    {{end}}
  }
  {{if .ssl_enabled}}
  server {
  	listen 443 ssl;
    ssl_certificate       {{.ssl_certificate}};
    ssl_certificate_key   {{.ssl_certificate_key}};
    ssl_session_timeout   5m;
    ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
    ssl_prefer_server_ciphers on;
    ssl_ciphers ECDH+AES256:ECDH+AES128:DH+3DES:!ADH:!AECDH:!MD5@SECLEVEL=1;
    
    access_log {{.access_log_path}};
    error_log {{.error_log_path}};

    gzip on;
    gzip_types text/plain test/csv application/json;
   	
    location /download {
       alias {{.repo}};
    }
  }
  {{end}}
}
`
