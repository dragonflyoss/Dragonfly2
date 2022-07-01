% DFGET(1) Version v2.0.4 | Frivolous "Dfget" Documentation

NAME
====

**dfget** — client of Dragonfly used to download and upload files

SYNOPSIS
========

dfget is the client of Dragonfly which takes a role of peer in a P2P network. When user triggers a file downloading
task, dfget will download the pieces of file from other peers. Meanwhile, it will act as an uploader to support other
peers to download pieces from it if it owns them. In addition, dfget has the abilities to provide more advanced
functionality, such as network bandwidth limit, transmission encryption and so on.

Options
-------

--alivetime

:   alive duration for which uploader keeps no accessing by any uploading requests, after this period uploader will automatically exit (default 5m0s)

--cacerts

:   the cacert file which is used to verify remote server when supernode interact with the source.

--callsystem

:   the name of dfget caller which is for debugging. Once set, it will be passed to all components around the request to make debugging easy

--clientqueue

:   specify the size of client queue which controls the number of pieces that can be processed simultaneously

--console

:   show log on console, it's conflict with '--showbar'

--daemon-pid

:   the daemon pid (default "/tmp/dfdaemon.pid")

--daemon-sock

:   the unix domain socket address for grpc with daemon (default "/tmp/dfdamon.sock")

--dfdaemon

:   identify whether the request is from dfdaemon

--expiretime

:   caching duration for which cached file keeps no accessed by any process, after this period cache file will be deleted (default 3m0s)

-f, --filter

:   filter some query params of URL, use char '&' to separate different params

--header

:   http header

-h, --help

:   help for dfget

--home

:   the work home directory of dfget (default "/Users/jim/.dragonfly/dfdaemon/")

-i, --identifier

:   the usage of identifier is making different downloading tasks generate different downloading task IDs even if they have the same URLs. conflict with --md5.

--insecure

:   identify whether supernode should skip secure verify when interact with the source.

--ip

:   IP address that server will listen on (default "0.0.0.0")

-m, --md5

:   md5 value input from user for the requested downloading file to enhance security

--more-daemon-options

:   more options passed to daemon by command line, please confirm your options with "dfget daemon --help"

-n, --node

:   deprecated, please use schedulers instead. specify the addresses(host:port=weight) of supernodes where the host is necessary, the port(default: 8002) and the weight(default:1) are optional. And the type of weight must be integer

--notbacksource

:   disable back source downloading for requested file when p2p fails to download it

-o, --output

:   destination path which is used to store the requested downloading file. It must contain detailed directory and specific filename, for example, '/tmp/file.mp4'

-p, --pattern

:   download pattern, must be p2p/seed-peer/source, seed-peer and source do not support flag --totallimit (default "p2p")

--port

:   port number that server will listen on (default 65002)

--schedulers

:   the scheduler addresses

-b, --showbar

:   show progress bar, it is conflict with '--console'

-e, --timeout

:   timeout set for file downloading task. If dfget has not finished downloading all pieces of file before --timeout, the dfget will throw an error and exit

--totallimit

:   network bandwidth rate limit for the whole host, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte (default 104857600.000000)

-u, --url

:   URL of user requested downloading file(only HTTP/HTTPs supported)

--verbose

:   enable verbose mode, all debug log will be display

BUGS
====

See GitHub Issues: <https://github.com/dragonflyoss/Dragonfly2/issues>
