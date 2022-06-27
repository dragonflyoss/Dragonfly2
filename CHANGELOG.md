<a name="unreleased"></a>
## [Unreleased]


<a name="v2.0.3"></a>
## [v2.0.3] - 2022-06-02
### Chore
- release v2.0.3 ([#1360](https://github.com/dragonflyoss/Dragonfly2/issues/1360))
- update content range for partial content ([#1357](https://github.com/dragonflyoss/Dragonfly2/issues/1357))

### Docs
- update readme system features ([#1359](https://github.com/dragonflyoss/Dragonfly2/issues/1359))

### Feat
- add seed peer e2e test ([#1358](https://github.com/dragonflyoss/Dragonfly2/issues/1358))
- update console and helm-charts submodule ([#1355](https://github.com/dragonflyoss/Dragonfly2/issues/1355))
- use uid/gid as UserID and UserGroup if current user not found in passwd ([#1352](https://github.com/dragonflyoss/Dragonfly2/issues/1352))
- use 127.0.0.1 as IPv4 if there's no external IPv4 addr ([#1353](https://github.com/dragonflyoss/Dragonfly2/issues/1353))


<a name="v2.0.3-beta.9"></a>
## [v2.0.3-beta.9] - 2022-06-01
### Feat
- update submodule


<a name="v2.0.3-beta.8"></a>
## [v2.0.3-beta.8] - 2022-05-31
### Chore
- add check size action ([#1350](https://github.com/dragonflyoss/Dragonfly2/issues/1350))

### Docs
- readme typo
- readme add seed peer ([#1349](https://github.com/dragonflyoss/Dragonfly2/issues/1349))

### Feat
- add security group id with scheduler cluster ([#1354](https://github.com/dragonflyoss/Dragonfly2/issues/1354))
- change pattern from cdn to seed peer and remove kustomize shell ([#1345](https://github.com/dragonflyoss/Dragonfly2/issues/1345))

### Fix
- register fail panic ([#1351](https://github.com/dragonflyoss/Dragonfly2/issues/1351))
- find partial completed overflow ([#1346](https://github.com/dragonflyoss/Dragonfly2/issues/1346))


<a name="v2.0.3-beta.7"></a>
## [v2.0.3-beta.7] - 2022-05-31
### Chore
- check large files in pull request ([#1332](https://github.com/dragonflyoss/Dragonfly2/issues/1332))
- add target peer id in sync piece trace ([#1278](https://github.com/dragonflyoss/Dragonfly2/issues/1278))
- optimize create synchronizer logic ([#1269](https://github.com/dragonflyoss/Dragonfly2/issues/1269))
- add sync pieces trace and update sync pieces logic for done task ([#1263](https://github.com/dragonflyoss/Dragonfly2/issues/1263))
- add schedule cron with e2e testing ([#1262](https://github.com/dragonflyoss/Dragonfly2/issues/1262))
- optimize sync pieces ([#1253](https://github.com/dragonflyoss/Dragonfly2/issues/1253))
- update pull request template ([#1251](https://github.com/dragonflyoss/Dragonfly2/issues/1251))

### Feat
- update casbin/gorm-adapter version and change e2e charts config
- update helm charts
- update dependencies
- add seed peer metrics ([#1342](https://github.com/dragonflyoss/Dragonfly2/issues/1342))
- grpc health probe support arm64 ([#1338](https://github.com/dragonflyoss/Dragonfly2/issues/1338))
- docker build with multi platforms ([#1337](https://github.com/dragonflyoss/Dragonfly2/issues/1337))
- add sync piece watchdog ([#1272](https://github.com/dragonflyoss/Dragonfly2/issues/1272))
- scheduler handles seed peer failed ([#1325](https://github.com/dragonflyoss/Dragonfly2/issues/1325))
- custom preheat tag parameters ([#1324](https://github.com/dragonflyoss/Dragonfly2/issues/1324))
- client add tls verify config ([#1323](https://github.com/dragonflyoss/Dragonfly2/issues/1323))
- scheduler register interface return task type ([#1318](https://github.com/dragonflyoss/Dragonfly2/issues/1318))
- get active peer count ([#1315](https://github.com/dragonflyoss/Dragonfly2/issues/1315))
- reduce dynconfig log ([#1312](https://github.com/dragonflyoss/Dragonfly2/issues/1312))
- back source when receive seed request ([#1309](https://github.com/dragonflyoss/Dragonfly2/issues/1309))
- update helm charts submodule ([#1308](https://github.com/dragonflyoss/Dragonfly2/issues/1308))
- add vsock network type support ([#1303](https://github.com/dragonflyoss/Dragonfly2/issues/1303))
- support response header ([#1292](https://github.com/dragonflyoss/Dragonfly2/issues/1292))
- add seed peer logic ([#1302](https://github.com/dragonflyoss/Dragonfly2/issues/1302))
- support more digest like sha256 ([#1304](https://github.com/dragonflyoss/Dragonfly2/issues/1304))
- implement pattern in client daemon ([#1231](https://github.com/dragonflyoss/Dragonfly2/issues/1231))
- scheduler add seed peer ([#1298](https://github.com/dragonflyoss/Dragonfly2/issues/1298))
- don't gc client rpc connection if ExpireTime is 0 ([#1296](https://github.com/dragonflyoss/Dragonfly2/issues/1296))
- update scheduler PeerResult validation ([#1294](https://github.com/dragonflyoss/Dragonfly2/issues/1294))
- manager add seed peer ([#1293](https://github.com/dragonflyoss/Dragonfly2/issues/1293))
- implement client seed mode ([#1247](https://github.com/dragonflyoss/Dragonfly2/issues/1247))
- scheduler peer result validation ([#1288](https://github.com/dragonflyoss/Dragonfly2/issues/1288))
- use a golang native file server to replace nginx ([#1258](https://github.com/dragonflyoss/Dragonfly2/issues/1258))
- support build arm&&arm64 dfget ([#1286](https://github.com/dragonflyoss/Dragonfly2/issues/1286))
- update filter parent ([#1279](https://github.com/dragonflyoss/Dragonfly2/issues/1279))
- in tree plugin ([#1276](https://github.com/dragonflyoss/Dragonfly2/issues/1276))
- move dfnet to pkg dir ([#1265](https://github.com/dragonflyoss/Dragonfly2/issues/1265))
- add dfcache rpm/deb packages and man pages and publish in goreleaser ([#1259](https://github.com/dragonflyoss/Dragonfly2/issues/1259))
- add AnnounceTask and StatTask metrics ([#1256](https://github.com/dragonflyoss/Dragonfly2/issues/1256))
- define and implement new dfdaemon APIs to make dragonfly2 work as a distributed cache ([#1227](https://github.com/dragonflyoss/Dragonfly2/issues/1227))
- redirect daemon stdout stderr to file ([#1244](https://github.com/dragonflyoss/Dragonfly2/issues/1244))
- registerTask returns to the task in time ([#1250](https://github.com/dragonflyoss/Dragonfly2/issues/1250))

### Fix
- e2e charts config
- seed peer reuse value
- dfdaemon seed peer metrics namespace ([#1343](https://github.com/dragonflyoss/Dragonfly2/issues/1343))
- create_at timestamp ([#1341](https://github.com/dragonflyoss/Dragonfly2/issues/1341))
- reuse seed peer id is not exist ([#1335](https://github.com/dragonflyoss/Dragonfly2/issues/1335))
- reuse non-end range request ([#1333](https://github.com/dragonflyoss/Dragonfly2/issues/1333))
- http range header validation ([#1334](https://github.com/dragonflyoss/Dragonfly2/issues/1334))
- nfpms maintainer ([#1326](https://github.com/dragonflyoss/Dragonfly2/issues/1326))
- reuse seed panic ([#1319](https://github.com/dragonflyoss/Dragonfly2/issues/1319))
- seed peer did not send done seed result and no content length send ([#1316](https://github.com/dragonflyoss/Dragonfly2/issues/1316))
- remove container after generating protoc ([#1306](https://github.com/dragonflyoss/Dragonfly2/issues/1306))
- digest reader and unit tests ([#1305](https://github.com/dragonflyoss/Dragonfly2/issues/1305))
- scheduler typo ([#1297](https://github.com/dragonflyoss/Dragonfly2/issues/1297))
- keep accept header ([#1291](https://github.com/dragonflyoss/Dragonfly2/issues/1291))
- grpc total_piece_count comment ([#1289](https://github.com/dragonflyoss/Dragonfly2/issues/1289))
- run.sh threw error on mac ([#1285](https://github.com/dragonflyoss/Dragonfly2/issues/1285))
- docker compose run.sh ([#1282](https://github.com/dragonflyoss/Dragonfly2/issues/1282))
- legacy cdn peer ([#1283](https://github.com/dragonflyoss/Dragonfly2/issues/1283))
- filter parent condition ([#1277](https://github.com/dragonflyoss/Dragonfly2/issues/1277))
- dfget daemon console log invalid ([#1275](https://github.com/dragonflyoss/Dragonfly2/issues/1275))
- scheduler config validation ([#1274](https://github.com/dragonflyoss/Dragonfly2/issues/1274))
- run.sh threw error on mac ([#1273](https://github.com/dragonflyoss/Dragonfly2/issues/1273))
- tree infinite loop ([#1271](https://github.com/dragonflyoss/Dragonfly2/issues/1271))
- acquire empty dst pid ([#1268](https://github.com/dragonflyoss/Dragonfly2/issues/1268))
- skip unsupported kernel in systemd service ([#1261](https://github.com/dragonflyoss/Dragonfly2/issues/1261))
- client synchronizer report error lock and dial grpc timeout ([#1260](https://github.com/dragonflyoss/Dragonfly2/issues/1260))
- prevent traversal tree from infinite loop ([#1266](https://github.com/dragonflyoss/Dragonfly2/issues/1266))
- error message ([#1255](https://github.com/dragonflyoss/Dragonfly2/issues/1255))
- client sync piece panic ([#1246](https://github.com/dragonflyoss/Dragonfly2/issues/1246))
- client superfluous usage gc ([#1243](https://github.com/dragonflyoss/Dragonfly2/issues/1243))

### Refactor
- scheduler grpc ([#1310](https://github.com/dragonflyoss/Dragonfly2/issues/1310))
- scheduler task SizeScope ([#1287](https://github.com/dragonflyoss/Dragonfly2/issues/1287))

### Test
- update e2e charts config
- watchdog
- close dfget back-to-souce ([#1317](https://github.com/dragonflyoss/Dragonfly2/issues/1317))
- fix storage backups ([#1270](https://github.com/dragonflyoss/Dragonfly2/issues/1270))
- scheduler storage ([#1257](https://github.com/dragonflyoss/Dragonfly2/issues/1257))
- AnnounceTask and StatTask ([#1254](https://github.com/dragonflyoss/Dragonfly2/issues/1254))


<a name="v2.0.2"></a>
## [v2.0.2] - 2022-05-27
### Feat
- grpc health probe support arm64 ([#1338](https://github.com/dragonflyoss/Dragonfly2/issues/1338))
- support build arm&&arm64 dfget ([#1286](https://github.com/dragonflyoss/Dragonfly2/issues/1286))
- docker build with multi platforms ([#1337](https://github.com/dragonflyoss/Dragonfly2/issues/1337))
- redirect daemon stdout stderr to file ([#1244](https://github.com/dragonflyoss/Dragonfly2/issues/1244))
- registerTask returns to the task in time ([#1250](https://github.com/dragonflyoss/Dragonfly2/issues/1250))

### Fix
- nfpms maintainer
- client sync piece panic ([#1246](https://github.com/dragonflyoss/Dragonfly2/issues/1246))
- client superfluous usage gc ([#1243](https://github.com/dragonflyoss/Dragonfly2/issues/1243))


<a name="v2.0.3-beta.6"></a>
## [v2.0.3-beta.6] - 2022-05-26
### Feat
- remove test
- test
- docker build with multi platforms


<a name="v2.0.3-beta.5"></a>
## [v2.0.3-beta.5] - 2022-05-25
### Chore
- check large files in pull request ([#1332](https://github.com/dragonflyoss/Dragonfly2/issues/1332))

### Fix
- reuse seed peer id is not exist ([#1335](https://github.com/dragonflyoss/Dragonfly2/issues/1335))
- reuse non-end range request ([#1333](https://github.com/dragonflyoss/Dragonfly2/issues/1333))
- http range header validation ([#1334](https://github.com/dragonflyoss/Dragonfly2/issues/1334))


<a name="v2.0.3-beta.4"></a>
## [v2.0.3-beta.4] - 2022-05-24
### Feat
- add sync piece watchdog ([#1272](https://github.com/dragonflyoss/Dragonfly2/issues/1272))
- scheduler handles seed peer failed ([#1325](https://github.com/dragonflyoss/Dragonfly2/issues/1325))
- custom preheat tag parameters ([#1324](https://github.com/dragonflyoss/Dragonfly2/issues/1324))
- client add tls verify config ([#1323](https://github.com/dragonflyoss/Dragonfly2/issues/1323))
- scheduler register interface return task type ([#1318](https://github.com/dragonflyoss/Dragonfly2/issues/1318))
- get active peer count ([#1315](https://github.com/dragonflyoss/Dragonfly2/issues/1315))
- reduce dynconfig log ([#1312](https://github.com/dragonflyoss/Dragonfly2/issues/1312))
- back source when receive seed request ([#1309](https://github.com/dragonflyoss/Dragonfly2/issues/1309))
- update helm charts submodule ([#1308](https://github.com/dragonflyoss/Dragonfly2/issues/1308))
- add vsock network type support ([#1303](https://github.com/dragonflyoss/Dragonfly2/issues/1303))
- support response header ([#1292](https://github.com/dragonflyoss/Dragonfly2/issues/1292))
- add seed peer logic ([#1302](https://github.com/dragonflyoss/Dragonfly2/issues/1302))
- support more digest like sha256 ([#1304](https://github.com/dragonflyoss/Dragonfly2/issues/1304))
- implement pattern in client daemon ([#1231](https://github.com/dragonflyoss/Dragonfly2/issues/1231))
- scheduler add seed peer ([#1298](https://github.com/dragonflyoss/Dragonfly2/issues/1298))
- don't gc client rpc connection if ExpireTime is 0 ([#1296](https://github.com/dragonflyoss/Dragonfly2/issues/1296))
- update scheduler PeerResult validation ([#1294](https://github.com/dragonflyoss/Dragonfly2/issues/1294))
- manager add seed peer ([#1293](https://github.com/dragonflyoss/Dragonfly2/issues/1293))
- implement client seed mode ([#1247](https://github.com/dragonflyoss/Dragonfly2/issues/1247))
- scheduler peer result validation ([#1288](https://github.com/dragonflyoss/Dragonfly2/issues/1288))
- use a golang native file server to replace nginx ([#1258](https://github.com/dragonflyoss/Dragonfly2/issues/1258))
- support build arm&&arm64 dfget ([#1286](https://github.com/dragonflyoss/Dragonfly2/issues/1286))

### Fix
- nfpms maintainer ([#1326](https://github.com/dragonflyoss/Dragonfly2/issues/1326))
- reuse seed panic ([#1319](https://github.com/dragonflyoss/Dragonfly2/issues/1319))
- seed peer did not send done seed result and no content length send ([#1316](https://github.com/dragonflyoss/Dragonfly2/issues/1316))
- remove container after generating protoc ([#1306](https://github.com/dragonflyoss/Dragonfly2/issues/1306))
- digest reader and unit tests ([#1305](https://github.com/dragonflyoss/Dragonfly2/issues/1305))
- scheduler typo ([#1297](https://github.com/dragonflyoss/Dragonfly2/issues/1297))
- keep accept header ([#1291](https://github.com/dragonflyoss/Dragonfly2/issues/1291))
- grpc total_piece_count comment ([#1289](https://github.com/dragonflyoss/Dragonfly2/issues/1289))
- run.sh threw error on mac ([#1285](https://github.com/dragonflyoss/Dragonfly2/issues/1285))

### Refactor
- scheduler grpc ([#1310](https://github.com/dragonflyoss/Dragonfly2/issues/1310))
- scheduler task SizeScope ([#1287](https://github.com/dragonflyoss/Dragonfly2/issues/1287))

### Test
- watchdog
- close dfget back-to-souce ([#1317](https://github.com/dragonflyoss/Dragonfly2/issues/1317))


<a name="v2.0.3-alpha.1"></a>
## [v2.0.3-alpha.1] - 2022-05-24
### Fix
- nfpms maintainer ([#1326](https://github.com/dragonflyoss/Dragonfly2/issues/1326))
- register fail panic ([#1351](https://github.com/dragonflyoss/Dragonfly2/issues/1351))
- reuse non-end range request ([#1333](https://github.com/dragonflyoss/Dragonfly2/issues/1333))
- find partial completed overflow ([#1346](https://github.com/dragonflyoss/Dragonfly2/issues/1346))
- http range header validation ([#1334](https://github.com/dragonflyoss/Dragonfly2/issues/1334))


<a name="v2.0.3-beta.3"></a>
## [v2.0.3-beta.3] - 2022-05-17
### Feat
- generate protoc
- update helm charts submodule ([#1308](https://github.com/dragonflyoss/Dragonfly2/issues/1308))
- add vsock network type support ([#1303](https://github.com/dragonflyoss/Dragonfly2/issues/1303))
- support response header ([#1292](https://github.com/dragonflyoss/Dragonfly2/issues/1292))

### Fix
- remove container after generating protoc ([#1306](https://github.com/dragonflyoss/Dragonfly2/issues/1306))

### Refactor
- scheduler grpc


<a name="v2.0.3-beta.2"></a>
## [v2.0.3-beta.2] - 2022-05-16
### Feat
- add seed peer logic ([#1302](https://github.com/dragonflyoss/Dragonfly2/issues/1302))


<a name="v2.0.3-beta.1"></a>
## [v2.0.3-beta.1] - 2022-05-13
### Feat
- change docker compose
- remove cdn compatibility
- support more digest like sha256 ([#1304](https://github.com/dragonflyoss/Dragonfly2/issues/1304))
- go generate mocks
- dfdaemon change host uuid to host id
- implement pattern in client daemon ([#1231](https://github.com/dragonflyoss/Dragonfly2/issues/1231))
- remove cdn job
- remove cdn logic
- announce seed peer
- scheduler add seed peer ([#1298](https://github.com/dragonflyoss/Dragonfly2/issues/1298))
- don't gc client rpc connection if ExpireTime is 0 ([#1296](https://github.com/dragonflyoss/Dragonfly2/issues/1296))
- update scheduler PeerResult validation ([#1294](https://github.com/dragonflyoss/Dragonfly2/issues/1294))
- manager add seed peer ([#1293](https://github.com/dragonflyoss/Dragonfly2/issues/1293))

### Fix
- reuse panic
- digest reader and unit tests ([#1305](https://github.com/dragonflyoss/Dragonfly2/issues/1305))
- scheduler typo ([#1297](https://github.com/dragonflyoss/Dragonfly2/issues/1297))


<a name="v2.0.3-beta.0"></a>
## [v2.0.3-beta.0] - 2022-05-06
### Chore
- add target peer id in sync piece trace ([#1278](https://github.com/dragonflyoss/Dragonfly2/issues/1278))

### Feat
- implement client seed mode ([#1247](https://github.com/dragonflyoss/Dragonfly2/issues/1247))
- scheduler peer result validation ([#1288](https://github.com/dragonflyoss/Dragonfly2/issues/1288))
- use a golang native file server to replace nginx ([#1258](https://github.com/dragonflyoss/Dragonfly2/issues/1258))
- support build arm&&arm64 dfget ([#1286](https://github.com/dragonflyoss/Dragonfly2/issues/1286))
- update filter parent ([#1279](https://github.com/dragonflyoss/Dragonfly2/issues/1279))

### Fix
- keep accept header ([#1291](https://github.com/dragonflyoss/Dragonfly2/issues/1291))
- grpc total_piece_count comment ([#1289](https://github.com/dragonflyoss/Dragonfly2/issues/1289))
- run.sh threw error on mac ([#1285](https://github.com/dragonflyoss/Dragonfly2/issues/1285))
- docker compose run.sh ([#1282](https://github.com/dragonflyoss/Dragonfly2/issues/1282))
- legacy cdn peer ([#1283](https://github.com/dragonflyoss/Dragonfly2/issues/1283))
- filter parent condition ([#1277](https://github.com/dragonflyoss/Dragonfly2/issues/1277))

### Refactor
- scheduler task SizeScope ([#1287](https://github.com/dragonflyoss/Dragonfly2/issues/1287))


<a name="v2.0.3-alpha.0"></a>
## [v2.0.3-alpha.0] - 2022-04-24
### Chore
- optimize create synchronizer logic ([#1269](https://github.com/dragonflyoss/Dragonfly2/issues/1269))
- add sync pieces trace and update sync pieces logic for done task ([#1263](https://github.com/dragonflyoss/Dragonfly2/issues/1263))
- add schedule cron with e2e testing ([#1262](https://github.com/dragonflyoss/Dragonfly2/issues/1262))
- optimize sync pieces ([#1253](https://github.com/dragonflyoss/Dragonfly2/issues/1253))
- update pull request template ([#1251](https://github.com/dragonflyoss/Dragonfly2/issues/1251))
- update compatibility version to v2.0.2
- update helm-charts commit
- generate change log
- update manager console commit ([#1219](https://github.com/dragonflyoss/Dragonfly2/issues/1219))
- print client stream task error log ([#1210](https://github.com/dragonflyoss/Dragonfly2/issues/1210))
- report client back source error ([#1209](https://github.com/dragonflyoss/Dragonfly2/issues/1209))

### Docs
- move document from /docs to d7y.io ([#1229](https://github.com/dragonflyoss/Dragonfly2/issues/1229))

### Feat
- in tree plugin ([#1276](https://github.com/dragonflyoss/Dragonfly2/issues/1276))
- move dfnet to pkg dir ([#1265](https://github.com/dragonflyoss/Dragonfly2/issues/1265))
- add dfcache rpm/deb packages and man pages and publish in goreleaser ([#1259](https://github.com/dragonflyoss/Dragonfly2/issues/1259))
- add AnnounceTask and StatTask metrics ([#1256](https://github.com/dragonflyoss/Dragonfly2/issues/1256))
- define and implement new dfdaemon APIs to make dragonfly2 work as a distributed cache ([#1227](https://github.com/dragonflyoss/Dragonfly2/issues/1227))
- redirect daemon stdout stderr to file ([#1244](https://github.com/dragonflyoss/Dragonfly2/issues/1244))
- registerTask returns to the task in time ([#1250](https://github.com/dragonflyoss/Dragonfly2/issues/1250))
- docker-compose write log to file ([#1236](https://github.com/dragonflyoss/Dragonfly2/issues/1236))
- update docker compose version ([#1235](https://github.com/dragonflyoss/Dragonfly2/issues/1235))
- update to v2.0.2 ([#1232](https://github.com/dragonflyoss/Dragonfly2/issues/1232))
- scheduler blocks steal peers ([#1224](https://github.com/dragonflyoss/Dragonfly2/issues/1224))
- update manager console ([#1222](https://github.com/dragonflyoss/Dragonfly2/issues/1222))
- manager validate with config ([#1218](https://github.com/dragonflyoss/Dragonfly2/issues/1218))
- remove kustomize template ([#1216](https://github.com/dragonflyoss/Dragonfly2/issues/1216))
- add back source fail metric in client ([#1214](https://github.com/dragonflyoss/Dragonfly2/issues/1214))
- cannot delete a cluster with existing instances ([#1213](https://github.com/dragonflyoss/Dragonfly2/issues/1213))
- add type to DownloadFailureCount ([#1212](https://github.com/dragonflyoss/Dragonfly2/issues/1212))
- if the number of failed peers in the task is greater than FailedPeerCountLimit, then scheduler notifies running peers of failure ([#1211](https://github.com/dragonflyoss/Dragonfly2/issues/1211))
- optimize get available task ([#1208](https://github.com/dragonflyoss/Dragonfly2/issues/1208))

### Fix
- dfget daemon console log invalid ([#1275](https://github.com/dragonflyoss/Dragonfly2/issues/1275))
- scheduler config validation ([#1274](https://github.com/dragonflyoss/Dragonfly2/issues/1274))
- run.sh threw error on mac ([#1273](https://github.com/dragonflyoss/Dragonfly2/issues/1273))
- tree infinite loop ([#1271](https://github.com/dragonflyoss/Dragonfly2/issues/1271))
- acquire empty dst pid ([#1268](https://github.com/dragonflyoss/Dragonfly2/issues/1268))
- skip unsupported kernel in systemd service ([#1261](https://github.com/dragonflyoss/Dragonfly2/issues/1261))
- client synchronizer report error lock and dial grpc timeout ([#1260](https://github.com/dragonflyoss/Dragonfly2/issues/1260))
- prevent traversal tree from infinite loop ([#1266](https://github.com/dragonflyoss/Dragonfly2/issues/1266))
- error message ([#1255](https://github.com/dragonflyoss/Dragonfly2/issues/1255))
- client sync piece panic ([#1246](https://github.com/dragonflyoss/Dragonfly2/issues/1246))
- client superfluous usage gc ([#1243](https://github.com/dragonflyoss/Dragonfly2/issues/1243))
- client sync send unsafe call ([#1240](https://github.com/dragonflyoss/Dragonfly2/issues/1240))
- client unexpected timeout ([#1239](https://github.com/dragonflyoss/Dragonfly2/issues/1239))
- goreleaser config
- make generate ([#1228](https://github.com/dragonflyoss/Dragonfly2/issues/1228))
- calculate FreeUploadLoad ([#1226](https://github.com/dragonflyoss/Dragonfly2/issues/1226))
- sync pieces hang ([#1221](https://github.com/dragonflyoss/Dragonfly2/issues/1221))

### Test
- fix storage backups ([#1270](https://github.com/dragonflyoss/Dragonfly2/issues/1270))
- scheduler storage ([#1257](https://github.com/dragonflyoss/Dragonfly2/issues/1257))
- AnnounceTask and StatTask ([#1254](https://github.com/dragonflyoss/Dragonfly2/issues/1254))


<a name="v2.0.2-rc.27"></a>
## [v2.0.2-rc.27] - 2022-03-29
### Chore
- update workflows compatibility version ([#1192](https://github.com/dragonflyoss/Dragonfly2/issues/1192))

### Docs
- add slack and google groups ([#1203](https://github.com/dragonflyoss/Dragonfly2/issues/1203))

### Feat
- change scheduler and cdn listen ([#1205](https://github.com/dragonflyoss/Dragonfly2/issues/1205))
- scheduler add block peers set ([#1202](https://github.com/dragonflyoss/Dragonfly2/issues/1202))
- add grpc-health-probe to image ([#1196](https://github.com/dragonflyoss/Dragonfly2/issues/1196))
- add grpc health interface ([#1195](https://github.com/dragonflyoss/Dragonfly2/issues/1195))

### Fix
- client miss failed piece ([#1194](https://github.com/dragonflyoss/Dragonfly2/issues/1194))

### Refactor
- scheduler end and begin of piece ([#1189](https://github.com/dragonflyoss/Dragonfly2/issues/1189))


<a name="v2.0.2-rc.26"></a>
## [v2.0.2-rc.26] - 2022-03-25
### Chore
- change golangci-lint min-complexity value ([#1188](https://github.com/dragonflyoss/Dragonfly2/issues/1188))
- optimize stream peer task ([#1186](https://github.com/dragonflyoss/Dragonfly2/issues/1186))
- always fallback to legacy get pieces ([#1180](https://github.com/dragonflyoss/Dragonfly2/issues/1180))
- update go mod ([#1156](https://github.com/dragonflyoss/Dragonfly2/issues/1156))
- add makefile note ([#1155](https://github.com/dragonflyoss/Dragonfly2/issues/1155))
- change scheduler config ([#1140](https://github.com/dragonflyoss/Dragonfly2/issues/1140))
- fast back source when get pieces task failed ([#1123](https://github.com/dragonflyoss/Dragonfly2/issues/1123))
- optimize reuse logic ([#1110](https://github.com/dragonflyoss/Dragonfly2/issues/1110))
- init url meta in rpc server ([#1098](https://github.com/dragonflyoss/Dragonfly2/issues/1098))
- update gorelease ldflags ([#1086](https://github.com/dragonflyoss/Dragonfly2/issues/1086))
- enable range feature gate in e2e ([#1059](https://github.com/dragonflyoss/Dragonfly2/issues/1059))
- add content length for fast stream peer task ([#1061](https://github.com/dragonflyoss/Dragonfly2/issues/1061))
- optimize https pass through ([#1054](https://github.com/dragonflyoss/Dragonfly2/issues/1054))
- use buildx to build docker images in e2e ([#1018](https://github.com/dragonflyoss/Dragonfly2/issues/1018))
- add missing pod log volumes in e2e ([#1037](https://github.com/dragonflyoss/Dragonfly2/issues/1037))
- upgrade to ginkgo v2 ([#1036](https://github.com/dragonflyoss/Dragonfly2/issues/1036))
- add piece task metrics in daemon ([#1030](https://github.com/dragonflyoss/Dragonfly2/issues/1030))
- update outdated log ([#1028](https://github.com/dragonflyoss/Dragonfly2/issues/1028))
- optimize metrics and trace in daemon ([#1022](https://github.com/dragonflyoss/Dragonfly2/issues/1022))
- register to scheduler after updated running tasks ([#1016](https://github.com/dragonflyoss/Dragonfly2/issues/1016))
- optimize defer and test ([#1010](https://github.com/dragonflyoss/Dragonfly2/issues/1010))
- workflow add test timeout ([#1011](https://github.com/dragonflyoss/Dragonfly2/issues/1011))
- sync docker-compose scheduler config ([#1001](https://github.com/dragonflyoss/Dragonfly2/issues/1001))
- parameterize tests in peer task ([#994](https://github.com/dragonflyoss/Dragonfly2/issues/994))
- clarify daemon interface ([#991](https://github.com/dragonflyoss/Dragonfly2/issues/991))
- change docker.pkg.github.com to ghcr.io ([#973](https://github.com/dragonflyoss/Dragonfly2/issues/973))
- copy e2e proxy log to artifact ([#962](https://github.com/dragonflyoss/Dragonfly2/issues/962))
- add version metric ([#954](https://github.com/dragonflyoss/Dragonfly2/issues/954))
- optimize back source update digest logic ([#950](https://github.com/dragonflyoss/Dragonfly2/issues/950))
- support multi daemons e2e test ([#896](https://github.com/dragonflyoss/Dragonfly2/issues/896))
- update UnknownSourceFileLen ([#888](https://github.com/dragonflyoss/Dragonfly2/issues/888))
- update changelog

### Docs
- add plugin builder ([#1101](https://github.com/dragonflyoss/Dragonfly2/issues/1101))
- add metrics document ([#1075](https://github.com/dragonflyoss/Dragonfly2/issues/1075))
- add containerd private registry configuration ([#1074](https://github.com/dragonflyoss/Dragonfly2/issues/1074))
- add containerd private registry configuration ([#1073](https://github.com/dragonflyoss/Dragonfly2/issues/1073))
- add docs about preheat console ([#1072](https://github.com/dragonflyoss/Dragonfly2/issues/1072))
- manager installation ([#1063](https://github.com/dragonflyoss/Dragonfly2/issues/1063))
- update plugin doc ([#951](https://github.com/dragonflyoss/Dragonfly2/issues/951))
- update plugin docs ([#921](https://github.com/dragonflyoss/Dragonfly2/issues/921))
- dir path ([#904](https://github.com/dragonflyoss/Dragonfly2/issues/904))
- add plugin guide ([#875](https://github.com/dragonflyoss/Dragonfly2/issues/875))

### Feat
- remove grpc error code validate ([#1191](https://github.com/dragonflyoss/Dragonfly2/issues/1191))
- generate grpc protos in namely/protoc-all image ([#1187](https://github.com/dragonflyoss/Dragonfly2/issues/1187))
- scheduler addresses log ([#1183](https://github.com/dragonflyoss/Dragonfly2/issues/1183))
- manage GetCDN interface return scheduler info ([#1184](https://github.com/dragonflyoss/Dragonfly2/issues/1184))
- dfdaemon match scheduler with case insensitive ([#1181](https://github.com/dragonflyoss/Dragonfly2/issues/1181))
- add RBAC to manager config interface ([#1179](https://github.com/dragonflyoss/Dragonfly2/issues/1179))
- dfdaemon get available scheduler addresses in the same cluster ([#1178](https://github.com/dragonflyoss/Dragonfly2/issues/1178))
- implement grpc client side sync pieces ([#1167](https://github.com/dragonflyoss/Dragonfly2/issues/1167))
- seacher return multiple scheduler clusters ([#1175](https://github.com/dragonflyoss/Dragonfly2/issues/1175))
- replace time.Now().Sub by time.Since ([#1173](https://github.com/dragonflyoss/Dragonfly2/issues/1173))
- change DefaultServerOptions to variable
- change default scheduler filter parent limit ([#1166](https://github.com/dragonflyoss/Dragonfly2/issues/1166))
- implement bidirectional fetch pieces ([#1165](https://github.com/dragonflyoss/Dragonfly2/issues/1165))
- scheduler add default biz tag ([#1164](https://github.com/dragonflyoss/Dragonfly2/issues/1164))
- optimize proxy performance ([#1137](https://github.com/dragonflyoss/Dragonfly2/issues/1137))
- host remove peer ([#1161](https://github.com/dragonflyoss/Dragonfly2/issues/1161))
- change reschdule config ([#1158](https://github.com/dragonflyoss/Dragonfly2/issues/1158))
- update git submodule ([#1153](https://github.com/dragonflyoss/Dragonfly2/issues/1153))
- scheduler metrics add default value of biz tag ([#1151](https://github.com/dragonflyoss/Dragonfly2/issues/1151))
- add user update interface and rename rest to service ([#1148](https://github.com/dragonflyoss/Dragonfly2/issues/1148))
- scheduler trace trigger cdn ([#1147](https://github.com/dragonflyoss/Dragonfly2/issues/1147))
- add scheduler traffic metrics ([#1143](https://github.com/dragonflyoss/Dragonfly2/issues/1143))
- update otel package version and fix otelgrpc goroutine leak ([#1141](https://github.com/dragonflyoss/Dragonfly2/issues/1141))
- add scheduler metrics ([#1139](https://github.com/dragonflyoss/Dragonfly2/issues/1139))
- scheduler remove inactive host ([#1135](https://github.com/dragonflyoss/Dragonfly2/issues/1135))
- task state for register ([#1132](https://github.com/dragonflyoss/Dragonfly2/issues/1132))
- change grpc client keepalive config ([#1125](https://github.com/dragonflyoss/Dragonfly2/issues/1125))
- scheduler change piece cost from nanosecond to millisecond ([#1119](https://github.com/dragonflyoss/Dragonfly2/issues/1119))
- support health probe in daemon ([#1120](https://github.com/dragonflyoss/Dragonfly2/issues/1120))
- when peer downloads finished, peer deletes parent ([#1116](https://github.com/dragonflyoss/Dragonfly2/issues/1116))
- change source client dialer config ([#1115](https://github.com/dragonflyoss/Dragonfly2/issues/1115))
- optimize scheduler log ([#1114](https://github.com/dragonflyoss/Dragonfly2/issues/1114))
- remove needless manager grpc proxy ([#1113](https://github.com/dragonflyoss/Dragonfly2/issues/1113))
- set grpc logger verbosity from env variable ([#1111](https://github.com/dragonflyoss/Dragonfly2/issues/1111))
- change back-to-source timeout ([#1112](https://github.com/dragonflyoss/Dragonfly2/issues/1112))
- optimize scheduler ([#1106](https://github.com/dragonflyoss/Dragonfly2/issues/1106))
- reuse partial completed task ([#1107](https://github.com/dragonflyoss/Dragonfly2/issues/1107))
- optimize depth limit func ([#1102](https://github.com/dragonflyoss/Dragonfly2/issues/1102))
- change client default load limit ([#1104](https://github.com/dragonflyoss/Dragonfly2/issues/1104))
- limit tree depth ([#1099](https://github.com/dragonflyoss/Dragonfly2/issues/1099))
- update load limit ([#1097](https://github.com/dragonflyoss/Dragonfly2/issues/1097))
- optimize peer range ([#1095](https://github.com/dragonflyoss/Dragonfly2/issues/1095))
- add cdn addresses log ([#1091](https://github.com/dragonflyoss/Dragonfly2/issues/1091))
- scheduler add limit count of filter parent func ([#1090](https://github.com/dragonflyoss/Dragonfly2/issues/1090))
- merge ranged request storage into parent ([#1078](https://github.com/dragonflyoss/Dragonfly2/issues/1078))
- add dynamic parallel count ([#1088](https://github.com/dragonflyoss/Dragonfly2/issues/1088))
- fix docker-compose ([#1087](https://github.com/dragonflyoss/Dragonfly2/issues/1087))
- add prefetch metric in client ([#1068](https://github.com/dragonflyoss/Dragonfly2/issues/1068))
- when scheduler blocks cdn, resource does not initialize cdn ([#1081](https://github.com/dragonflyoss/Dragonfly2/issues/1081))
- scheduler blocks cdn ([#1079](https://github.com/dragonflyoss/Dragonfly2/issues/1079))
- job trigger cdn by resource ([#1076](https://github.com/dragonflyoss/Dragonfly2/issues/1076))
- add client request log ([#1069](https://github.com/dragonflyoss/Dragonfly2/issues/1069))
- support change console log level ([#1055](https://github.com/dragonflyoss/Dragonfly2/issues/1055))
- manager support mysql ssl connection ([#1015](https://github.com/dragonflyoss/Dragonfly2/issues/1015))
- remove host and task when peer make tree ([#1042](https://github.com/dragonflyoss/Dragonfly2/issues/1042))
- cdn download tiny file ([#1040](https://github.com/dragonflyoss/Dragonfly2/issues/1040))
- If cdn only updates IP, set cdn peers state to PeerStateLeave ([#1038](https://github.com/dragonflyoss/Dragonfly2/issues/1038))
- generate grpc protoc ([#1027](https://github.com/dragonflyoss/Dragonfly2/issues/1027))
- manager config model add is_boot key ([#1025](https://github.com/dragonflyoss/Dragonfly2/issues/1025))
- scheduler download tiny file with range header ([#1024](https://github.com/dragonflyoss/Dragonfly2/issues/1024))
- change compatibility version to v2.0.2-rc.0 ([#1017](https://github.com/dragonflyoss/Dragonfly2/issues/1017))
- when cdn peer is failed, peer should be back-to-source ([#1005](https://github.com/dragonflyoss/Dragonfly2/issues/1005))
- add actions job timout ([#1008](https://github.com/dragonflyoss/Dragonfly2/issues/1008))
- set peer state to running when scope size is SizeScope_TINY ([#1004](https://github.com/dragonflyoss/Dragonfly2/issues/1004))
- update submodule charts ([#1002](https://github.com/dragonflyoss/Dragonfly2/issues/1002))
- task mutex replace sync kmutex ([#1000](https://github.com/dragonflyoss/Dragonfly2/issues/1000))
- stream send error code ([#986](https://github.com/dragonflyoss/Dragonfly2/issues/986))
- trace https proxy request ([#996](https://github.com/dragonflyoss/Dragonfly2/issues/996))
- add scheduler host gc ([#989](https://github.com/dragonflyoss/Dragonfly2/issues/989))
- update typo in local_storage.go ([#955](https://github.com/dragonflyoss/Dragonfly2/issues/955))
- update charts submodule version ([#985](https://github.com/dragonflyoss/Dragonfly2/issues/985))
- change task and peer ttl ([#984](https://github.com/dragonflyoss/Dragonfly2/issues/984))
- when write last piece, generate digest ([#982](https://github.com/dragonflyoss/Dragonfly2/issues/982))
- merge same tasks in daemon ([#977](https://github.com/dragonflyoss/Dragonfly2/issues/977))
- if cdn is deleted, clear cdn related information ([#967](https://github.com/dragonflyoss/Dragonfly2/issues/967))
- add default DiskGCThresholdPercent and ignore it when is 0 ([#971](https://github.com/dragonflyoss/Dragonfly2/issues/971))
- improve redirect to allow url rewrite ([#969](https://github.com/dragonflyoss/Dragonfly2/issues/969))
- Add useProxies to registryMirror allowing to mirror more anything ([#965](https://github.com/dragonflyoss/Dragonfly2/issues/965))
- change metrics port to 8000 ([#964](https://github.com/dragonflyoss/Dragonfly2/issues/964))
- add daemon metrics support ([#960](https://github.com/dragonflyoss/Dragonfly2/issues/960))
- support disk usage gc in client ([#953](https://github.com/dragonflyoss/Dragonfly2/issues/953))
- update source.Response and source client interface ([#945](https://github.com/dragonflyoss/Dragonfly2/issues/945))
- remove stat log from scheduler ([#946](https://github.com/dragonflyoss/Dragonfly2/issues/946))
- support recursive download in dfget ([#932](https://github.com/dragonflyoss/Dragonfly2/issues/932))
- add kmutex and krwmutex ([#934](https://github.com/dragonflyoss/Dragonfly2/issues/934))
- make idgen package public ([#931](https://github.com/dragonflyoss/Dragonfly2/issues/931))
- make dfpath public ([#929](https://github.com/dragonflyoss/Dragonfly2/issues/929))
- dfdaemon list scheduler cluster with multi idc ([#917](https://github.com/dragonflyoss/Dragonfly2/issues/917))
- update submodule ([#916](https://github.com/dragonflyoss/Dragonfly2/issues/916))
- update task access time ([#909](https://github.com/dragonflyoss/Dragonfly2/issues/909))
- optmize dfget package upgrade support ([#804](https://github.com/dragonflyoss/Dragonfly2/issues/804))
- support create container without docker-compose ([#915](https://github.com/dragonflyoss/Dragonfly2/issues/915))
- add data directory ([#910](https://github.com/dragonflyoss/Dragonfly2/issues/910))
- add data storage directory  ([#907](https://github.com/dragonflyoss/Dragonfly2/issues/907))
- dfdaemon update content length ([#895](https://github.com/dragonflyoss/Dragonfly2/issues/895))
- lint sh ([#876](https://github.com/dragonflyoss/Dragonfly2/issues/876))

### Feature
- prefetch ranged requests ([#1053](https://github.com/dragonflyoss/Dragonfly2/issues/1053))
- support e2e feature gates ([#1056](https://github.com/dragonflyoss/Dragonfly2/issues/1056))
- change log level in-flight ([#1023](https://github.com/dragonflyoss/Dragonfly2/issues/1023))

### Ffix
- typo in Makefile ([#975](https://github.com/dragonflyoss/Dragonfly2/issues/975))

### Fix
- client break error ([#1190](https://github.com/dragonflyoss/Dragonfly2/issues/1190))
- rpc cdn sync piece tasks ([#1168](https://github.com/dragonflyoss/Dragonfly2/issues/1168))
- subscriber data race ([#1169](https://github.com/dragonflyoss/Dragonfly2/issues/1169))
- docker-compose run with mac throw error ([#1134](https://github.com/dragonflyoss/Dragonfly2/issues/1134))
- wrong md5 sign in cdn ([#1126](https://github.com/dragonflyoss/Dragonfly2/issues/1126))
- docker-compose preheat pending ([#1124](https://github.com/dragonflyoss/Dragonfly2/issues/1124))
- scheduler piece cost time ([#1118](https://github.com/dragonflyoss/Dragonfly2/issues/1118))
- when peer state is PeerStateSucceeded, return size scope is small ([#1103](https://github.com/dragonflyoss/Dragonfly2/issues/1103))
- delete peer's parent on PeerEventDownloadSucceeded event ([#1085](https://github.com/dragonflyoss/Dragonfly2/issues/1085))
- pull request template typo ([#1080](https://github.com/dragonflyoss/Dragonfly2/issues/1080))
- when cdn download failed, scheduler should set cdn peer state PeerStateFailed ([#1067](https://github.com/dragonflyoss/Dragonfly2/issues/1067))
- evaluate peer's parent ([#1064](https://github.com/dragonflyoss/Dragonfly2/issues/1064))
- scheduler download tiny file error ([#1052](https://github.com/dragonflyoss/Dragonfly2/issues/1052))
- docker actions typo ([#1041](https://github.com/dragonflyoss/Dragonfly2/issues/1041))
- cdn trigger peer error ([#1035](https://github.com/dragonflyoss/Dragonfly2/issues/1035))
- retrigger cdn panic ([#1034](https://github.com/dragonflyoss/Dragonfly2/issues/1034))
- calculate piece MD5 sign when last piece download ([#1006](https://github.com/dragonflyoss/Dragonfly2/issues/1006))
- register task with size scope ([#1003](https://github.com/dragonflyoss/Dragonfly2/issues/1003))
- when scheduler is not available, replace the scheduler client ([#999](https://github.com/dragonflyoss/Dragonfly2/issues/999))
- total pieces count not set cause digest invalid ([#992](https://github.com/dragonflyoss/Dragonfly2/issues/992))
- send piece result error not handled ([#987](https://github.com/dragonflyoss/Dragonfly2/issues/987))
- scheduler config typo ([#983](https://github.com/dragonflyoss/Dragonfly2/issues/983))
- schedulers send invalid direct piece ([#970](https://github.com/dragonflyoss/Dragonfly2/issues/970))
- use 'parent' as mainPeer in PeerPacket in removePeerFromCurrentTree() ([#957](https://github.com/dragonflyoss/Dragonfly2/issues/957))
- size scope empty ([#941](https://github.com/dragonflyoss/Dragonfly2/issues/941))
- not handle base.Code_SchedTaskStatusError in client ([#938](https://github.com/dragonflyoss/Dragonfly2/issues/938))
- infinitely get pieces when piece num is invalid ([#926](https://github.com/dragonflyoss/Dragonfly2/issues/926))
- plugin dir is empty ([#922](https://github.com/dragonflyoss/Dragonfly2/issues/922))
- peer gc ([#918](https://github.com/dragonflyoss/Dragonfly2/issues/918))
- go plugin test build error ([#912](https://github.com/dragonflyoss/Dragonfly2/issues/912))
- typo ([#911](https://github.com/dragonflyoss/Dragonfly2/issues/911))
- total pieces not set when back source ([#908](https://github.com/dragonflyoss/Dragonfly2/issues/908))
- mismatch digest peer task did not mark invalid ([#903](https://github.com/dragonflyoss/Dragonfly2/issues/903))
- dfget dfpath ([#901](https://github.com/dragonflyoss/Dragonfly2/issues/901))
- scheduler success event ([#891](https://github.com/dragonflyoss/Dragonfly2/issues/891))
- add cdn cluster to scheduler cluster ([#887](https://github.com/dragonflyoss/Dragonfly2/issues/887))
- small size task failed due to digest error ([#886](https://github.com/dragonflyoss/Dragonfly2/issues/886))
- searcher log ([#878](https://github.com/dragonflyoss/Dragonfly2/issues/878))

### Refactor
- manager grpc server ([#1047](https://github.com/dragonflyoss/Dragonfly2/issues/1047))
- scheduler grpc server ([#1046](https://github.com/dragonflyoss/Dragonfly2/issues/1046))
- docker workflows ([#1039](https://github.com/dragonflyoss/Dragonfly2/issues/1039))
- scheduler register task ([#924](https://github.com/dragonflyoss/Dragonfly2/issues/924))
- move from io/ioutil to io and os packages ([#906](https://github.com/dragonflyoss/Dragonfly2/issues/906))
- dfpath pkg ([#879](https://github.com/dragonflyoss/Dragonfly2/issues/879))

### Test
- fix e2e preheat case ([#1170](https://github.com/dragonflyoss/Dragonfly2/issues/1170))
- cache expire interval ([#1160](https://github.com/dragonflyoss/Dragonfly2/issues/1160))
- add scheduler constructSuccessPeerPacket case ([#1154](https://github.com/dragonflyoss/Dragonfly2/issues/1154))
- scheduler service handlePieceFail ([#1146](https://github.com/dragonflyoss/Dragonfly2/issues/1146))
- FilterParentCount ([#1094](https://github.com/dragonflyoss/Dragonfly2/issues/1094))
- scheduler handle failed piece ([#1084](https://github.com/dragonflyoss/Dragonfly2/issues/1084))
- dump goroutine in e2e ([#980](https://github.com/dragonflyoss/Dragonfly2/issues/980))
- idgen peer id ([#913](https://github.com/dragonflyoss/Dragonfly2/issues/913))


<a name="v2.0.1"></a>
## [v2.0.1] - 2022-03-22
### Docs
- add plugin guide ([#875](https://github.com/dragonflyoss/Dragonfly2/issues/875))

### Feat
- lint sh ([#876](https://github.com/dragonflyoss/Dragonfly2/issues/876))

### Fix
- add cdn cluster to scheduler cluster ([#887](https://github.com/dragonflyoss/Dragonfly2/issues/887))
- small size task failed due to digest error ([#886](https://github.com/dragonflyoss/Dragonfly2/issues/886))
- searcher log ([#878](https://github.com/dragonflyoss/Dragonfly2/issues/878))

### Reverts
- update fail register log


<a name="v2.0.2-rc.25"></a>
## [v2.0.2-rc.25] - 2022-03-16
### Feat
- scheduler add default biz tag ([#1164](https://github.com/dragonflyoss/Dragonfly2/issues/1164))
- optimize proxy performance ([#1137](https://github.com/dragonflyoss/Dragonfly2/issues/1137))


<a name="v2.0.2-rc.24"></a>
## [v2.0.2-rc.24] - 2022-03-15
### Chore
- update go mod ([#1156](https://github.com/dragonflyoss/Dragonfly2/issues/1156))
- add makefile note ([#1155](https://github.com/dragonflyoss/Dragonfly2/issues/1155))

### Feat
- host remove peer ([#1161](https://github.com/dragonflyoss/Dragonfly2/issues/1161))
- change reschdule config ([#1158](https://github.com/dragonflyoss/Dragonfly2/issues/1158))

### Test
- cache expire interval ([#1160](https://github.com/dragonflyoss/Dragonfly2/issues/1160))
- add scheduler constructSuccessPeerPacket case ([#1154](https://github.com/dragonflyoss/Dragonfly2/issues/1154))


<a name="v2.0.2-rc.23"></a>
## [v2.0.2-rc.23] - 2022-03-11
### Feat
- update git submodule ([#1153](https://github.com/dragonflyoss/Dragonfly2/issues/1153))
- scheduler metrics add default value of biz tag ([#1151](https://github.com/dragonflyoss/Dragonfly2/issues/1151))


<a name="v2.0.2-rc.22"></a>
## [v2.0.2-rc.22] - 2022-03-10
### Chore
- change scheduler config ([#1140](https://github.com/dragonflyoss/Dragonfly2/issues/1140))

### Feat
- add user update interface and rename rest to service ([#1148](https://github.com/dragonflyoss/Dragonfly2/issues/1148))
- scheduler trace trigger cdn ([#1147](https://github.com/dragonflyoss/Dragonfly2/issues/1147))
- add scheduler traffic metrics ([#1143](https://github.com/dragonflyoss/Dragonfly2/issues/1143))
- update otel package version and fix otelgrpc goroutine leak ([#1141](https://github.com/dragonflyoss/Dragonfly2/issues/1141))
- add scheduler metrics ([#1139](https://github.com/dragonflyoss/Dragonfly2/issues/1139))

### Test
- scheduler service handlePieceFail ([#1146](https://github.com/dragonflyoss/Dragonfly2/issues/1146))


<a name="v2.0.2-rc.21"></a>
## [v2.0.2-rc.21] - 2022-03-08
### Feat
- scheduler remove inactive host ([#1135](https://github.com/dragonflyoss/Dragonfly2/issues/1135))
- task state for register ([#1132](https://github.com/dragonflyoss/Dragonfly2/issues/1132))

### Fix
- docker-compose run with mac throw error ([#1134](https://github.com/dragonflyoss/Dragonfly2/issues/1134))


<a name="v2.0.2-rc.20"></a>
## [v2.0.2-rc.20] - 2022-03-04
### Fix
- wrong md5 sign in cdn ([#1126](https://github.com/dragonflyoss/Dragonfly2/issues/1126))


<a name="v2.0.2-rc.19"></a>
## [v2.0.2-rc.19] - 2022-03-04
### Chore
- fast back source when get pieces task failed ([#1123](https://github.com/dragonflyoss/Dragonfly2/issues/1123))

### Feat
- change grpc client keepalive config ([#1125](https://github.com/dragonflyoss/Dragonfly2/issues/1125))
- scheduler change piece cost from nanosecond to millisecond ([#1119](https://github.com/dragonflyoss/Dragonfly2/issues/1119))
- support health probe in daemon ([#1120](https://github.com/dragonflyoss/Dragonfly2/issues/1120))

### Fix
- docker-compose preheat pending ([#1124](https://github.com/dragonflyoss/Dragonfly2/issues/1124))


<a name="v2.0.2-rc.18"></a>
## [v2.0.2-rc.18] - 2022-03-03
### Chore
- optimize reuse logic ([#1110](https://github.com/dragonflyoss/Dragonfly2/issues/1110))

### Feat
- when peer downloads finished, peer deletes parent ([#1116](https://github.com/dragonflyoss/Dragonfly2/issues/1116))
- change source client dialer config ([#1115](https://github.com/dragonflyoss/Dragonfly2/issues/1115))
- optimize scheduler log ([#1114](https://github.com/dragonflyoss/Dragonfly2/issues/1114))
- remove needless manager grpc proxy ([#1113](https://github.com/dragonflyoss/Dragonfly2/issues/1113))
- set grpc logger verbosity from env variable ([#1111](https://github.com/dragonflyoss/Dragonfly2/issues/1111))
- change back-to-source timeout ([#1112](https://github.com/dragonflyoss/Dragonfly2/issues/1112))

### Fix
- scheduler piece cost time ([#1118](https://github.com/dragonflyoss/Dragonfly2/issues/1118))


<a name="v2.0.2-rc.17"></a>
## [v2.0.2-rc.17] - 2022-03-02
### Chore
- init url meta in rpc server ([#1098](https://github.com/dragonflyoss/Dragonfly2/issues/1098))

### Docs
- add plugin builder ([#1101](https://github.com/dragonflyoss/Dragonfly2/issues/1101))

### Feat
- optimize scheduler ([#1106](https://github.com/dragonflyoss/Dragonfly2/issues/1106))
- reuse partial completed task ([#1107](https://github.com/dragonflyoss/Dragonfly2/issues/1107))
- optimize depth limit func ([#1102](https://github.com/dragonflyoss/Dragonfly2/issues/1102))
- change client default load limit ([#1104](https://github.com/dragonflyoss/Dragonfly2/issues/1104))
- limit tree depth ([#1099](https://github.com/dragonflyoss/Dragonfly2/issues/1099))

### Fix
- when peer state is PeerStateSucceeded, return size scope is small ([#1103](https://github.com/dragonflyoss/Dragonfly2/issues/1103))


<a name="v2.0.2-rc.16"></a>
## [v2.0.2-rc.16] - 2022-02-28
### Feat
- limit tree depth


<a name="v2.0.2-rc.15"></a>
## [v2.0.2-rc.15] - 2022-02-28
### Feat
- limit tree depth
- update load limit ([#1097](https://github.com/dragonflyoss/Dragonfly2/issues/1097))


<a name="v2.0.2-rc.14"></a>
## [v2.0.2-rc.14] - 2022-02-25
### Feat
- optimize peer range ([#1095](https://github.com/dragonflyoss/Dragonfly2/issues/1095))

### Test
- FilterParentCount ([#1094](https://github.com/dragonflyoss/Dragonfly2/issues/1094))


<a name="v2.0.2-rc.13"></a>
## [v2.0.2-rc.13] - 2022-02-24

<a name="v2.0.2-rc.12"></a>
## [v2.0.2-rc.12] - 2022-02-24
### Feat
- add cdn addresses log ([#1091](https://github.com/dragonflyoss/Dragonfly2/issues/1091))
- scheduler add limit count of filter parent func ([#1090](https://github.com/dragonflyoss/Dragonfly2/issues/1090))


<a name="v2.0.2-rc.11"></a>
## [v2.0.2-rc.11] - 2022-02-23
### Feat
- merge ranged request storage into parent ([#1078](https://github.com/dragonflyoss/Dragonfly2/issues/1078))
- add dynamic parallel count ([#1088](https://github.com/dragonflyoss/Dragonfly2/issues/1088))
- fix docker-compose ([#1087](https://github.com/dragonflyoss/Dragonfly2/issues/1087))

### Fix
- delete peer's parent on PeerEventDownloadSucceeded event ([#1085](https://github.com/dragonflyoss/Dragonfly2/issues/1085))


<a name="v2.0.2-rc.10"></a>
## [v2.0.2-rc.10] - 2022-02-22
### Chore
- update gorelease ldflags ([#1086](https://github.com/dragonflyoss/Dragonfly2/issues/1086))

### Feat
- add prefetch metric in client ([#1068](https://github.com/dragonflyoss/Dragonfly2/issues/1068))

### Test
- scheduler handle failed piece ([#1084](https://github.com/dragonflyoss/Dragonfly2/issues/1084))


<a name="v2.0.2-rc.9"></a>
## [v2.0.2-rc.9] - 2022-02-17
### Feat
- when scheduler blocks cdn, resource does not initialize cdn ([#1081](https://github.com/dragonflyoss/Dragonfly2/issues/1081))

### Fix
- pull request template typo ([#1080](https://github.com/dragonflyoss/Dragonfly2/issues/1080))


<a name="v2.0.2-rc.8"></a>
## [v2.0.2-rc.8] - 2022-02-17
### Docs
- add metrics document ([#1075](https://github.com/dragonflyoss/Dragonfly2/issues/1075))
- add containerd private registry configuration ([#1074](https://github.com/dragonflyoss/Dragonfly2/issues/1074))
- add containerd private registry configuration ([#1073](https://github.com/dragonflyoss/Dragonfly2/issues/1073))
- add docs about preheat console ([#1072](https://github.com/dragonflyoss/Dragonfly2/issues/1072))

### Feat
- scheduler blocks cdn ([#1079](https://github.com/dragonflyoss/Dragonfly2/issues/1079))
- job trigger cdn by resource ([#1076](https://github.com/dragonflyoss/Dragonfly2/issues/1076))


<a name="v2.0.2-rc.7"></a>
## [v2.0.2-rc.7] - 2022-02-15
### Feat
- add client request log ([#1069](https://github.com/dragonflyoss/Dragonfly2/issues/1069))

### Fix
- when cdn download failed, scheduler should set cdn peer state PeerStateFailed ([#1067](https://github.com/dragonflyoss/Dragonfly2/issues/1067))


<a name="v2.0.2-rc.5"></a>
## [v2.0.2-rc.5] - 2022-02-14

<a name="v2.0.2-rc.6"></a>
## [v2.0.2-rc.6] - 2022-02-14
### Chore
- enable range feature gate in e2e ([#1059](https://github.com/dragonflyoss/Dragonfly2/issues/1059))
- add content length for fast stream peer task ([#1061](https://github.com/dragonflyoss/Dragonfly2/issues/1061))
- optimize https pass through ([#1054](https://github.com/dragonflyoss/Dragonfly2/issues/1054))

### Docs
- manager installation ([#1063](https://github.com/dragonflyoss/Dragonfly2/issues/1063))

### Feat
- support change console log level ([#1055](https://github.com/dragonflyoss/Dragonfly2/issues/1055))

### Feature
- prefetch ranged requests ([#1053](https://github.com/dragonflyoss/Dragonfly2/issues/1053))
- support e2e feature gates ([#1056](https://github.com/dragonflyoss/Dragonfly2/issues/1056))

### Fix
- evaluate peer's parent ([#1064](https://github.com/dragonflyoss/Dragonfly2/issues/1064))
- scheduler download tiny file error ([#1052](https://github.com/dragonflyoss/Dragonfly2/issues/1052))


<a name="v2.0.2-rc.4"></a>
## [v2.0.2-rc.4] - 2022-01-29
### Feat
- manager support mysql ssl connection ([#1015](https://github.com/dragonflyoss/Dragonfly2/issues/1015))
- remove host and task when peer make tree ([#1042](https://github.com/dragonflyoss/Dragonfly2/issues/1042))
- cdn download tiny file ([#1040](https://github.com/dragonflyoss/Dragonfly2/issues/1040))

### Refactor
- manager grpc server ([#1047](https://github.com/dragonflyoss/Dragonfly2/issues/1047))
- scheduler grpc server ([#1046](https://github.com/dragonflyoss/Dragonfly2/issues/1046))


<a name="v2.0.2-rc.3"></a>
## [v2.0.2-rc.3] - 2022-01-25
### Chore
- use buildx to build docker images in e2e ([#1018](https://github.com/dragonflyoss/Dragonfly2/issues/1018))
- add missing pod log volumes in e2e ([#1037](https://github.com/dragonflyoss/Dragonfly2/issues/1037))
- upgrade to ginkgo v2 ([#1036](https://github.com/dragonflyoss/Dragonfly2/issues/1036))
- add piece task metrics in daemon ([#1030](https://github.com/dragonflyoss/Dragonfly2/issues/1030))

### Feat
- If cdn only updates IP, set cdn peers state to PeerStateLeave ([#1038](https://github.com/dragonflyoss/Dragonfly2/issues/1038))

### Fix
- docker actions typo ([#1041](https://github.com/dragonflyoss/Dragonfly2/issues/1041))
- cdn trigger peer error ([#1035](https://github.com/dragonflyoss/Dragonfly2/issues/1035))
- retrigger cdn panic ([#1034](https://github.com/dragonflyoss/Dragonfly2/issues/1034))

### Refactor
- docker workflows ([#1039](https://github.com/dragonflyoss/Dragonfly2/issues/1039))


<a name="v2.0.2-rc.2"></a>
## [v2.0.2-rc.2] - 2022-01-21
### Chore
- update outdated log ([#1028](https://github.com/dragonflyoss/Dragonfly2/issues/1028))
- optimize metrics and trace in daemon ([#1022](https://github.com/dragonflyoss/Dragonfly2/issues/1022))

### Feat
- generate grpc protoc ([#1027](https://github.com/dragonflyoss/Dragonfly2/issues/1027))
- manager config model add is_boot key ([#1025](https://github.com/dragonflyoss/Dragonfly2/issues/1025))
- scheduler download tiny file with range header ([#1024](https://github.com/dragonflyoss/Dragonfly2/issues/1024))

### Feature
- change log level in-flight ([#1023](https://github.com/dragonflyoss/Dragonfly2/issues/1023))


<a name="v2.0.2-rc.1"></a>
## [v2.0.2-rc.1] - 2022-01-20
### Feat
- change compatibility version to v2.0.2-rc.0 ([#1017](https://github.com/dragonflyoss/Dragonfly2/issues/1017))


<a name="v2.0.2-rc.0"></a>
## [v2.0.2-rc.0] - 2022-01-20
### Chore
- register to scheduler after updated running tasks ([#1016](https://github.com/dragonflyoss/Dragonfly2/issues/1016))


<a name="v2.0.2-beta.6"></a>
## [v2.0.2-beta.6] - 2022-01-20
### Feat
- when cdn peer is failed, peer should be back-to-source ([#1005](https://github.com/dragonflyoss/Dragonfly2/issues/1005))


<a name="v2.0.2-beta.5"></a>
## [v2.0.2-beta.5] - 2022-01-20
### Feat
- when cdn peer is failed, peer back-to-source
- schdule peer with cdn failed

### Test
- callback


<a name="v2.0.2-beta.4"></a>
## [v2.0.2-beta.4] - 2022-01-20
### Feat
- scheduler handle begin of piece

### Test
- trigger cdn task


<a name="v2.0.2-beta.3"></a>
## [v2.0.2-beta.3] - 2022-01-20
### Chore
- optimize defer and test ([#1010](https://github.com/dragonflyoss/Dragonfly2/issues/1010))
- workflow add test timeout ([#1011](https://github.com/dragonflyoss/Dragonfly2/issues/1011))
- sync docker-compose scheduler config ([#1001](https://github.com/dragonflyoss/Dragonfly2/issues/1001))
- parameterize tests in peer task ([#994](https://github.com/dragonflyoss/Dragonfly2/issues/994))

### Feat
- add actions job timout ([#1008](https://github.com/dragonflyoss/Dragonfly2/issues/1008))
- set peer state to running when scope size is SizeScope_TINY ([#1004](https://github.com/dragonflyoss/Dragonfly2/issues/1004))
- update submodule charts ([#1002](https://github.com/dragonflyoss/Dragonfly2/issues/1002))
- task mutex replace sync kmutex ([#1000](https://github.com/dragonflyoss/Dragonfly2/issues/1000))
- stream send error code ([#986](https://github.com/dragonflyoss/Dragonfly2/issues/986))
- trace https proxy request ([#996](https://github.com/dragonflyoss/Dragonfly2/issues/996))

### Fix
- calculate piece MD5 sign when last piece download ([#1006](https://github.com/dragonflyoss/Dragonfly2/issues/1006))
- register task with size scope ([#1003](https://github.com/dragonflyoss/Dragonfly2/issues/1003))
- when scheduler is not available, replace the scheduler client ([#999](https://github.com/dragonflyoss/Dragonfly2/issues/999))


<a name="v2.0.2-beta.2"></a>
## [v2.0.2-beta.2] - 2022-01-14
### Chore
- clarify daemon interface ([#991](https://github.com/dragonflyoss/Dragonfly2/issues/991))

### Feat
- dfdaemon report successful piece before end of piece
- add scheduler host gc ([#989](https://github.com/dragonflyoss/Dragonfly2/issues/989))
- update typo in local_storage.go ([#955](https://github.com/dragonflyoss/Dragonfly2/issues/955))
- add retry interval
- update charts submodule version ([#985](https://github.com/dragonflyoss/Dragonfly2/issues/985))
- update helm charts version
- send error code
- change task and peer ttl ([#984](https://github.com/dragonflyoss/Dragonfly2/issues/984))

### Fix
- total pieces count not set cause digest invalid ([#992](https://github.com/dragonflyoss/Dragonfly2/issues/992))
- send piece result error not handled ([#987](https://github.com/dragonflyoss/Dragonfly2/issues/987))

### Test
- callback


<a name="v2.0.2-beta.1"></a>
## [v2.0.2-beta.1] - 2022-01-12
### Feat
- change task and peer ttl ([#984](https://github.com/dragonflyoss/Dragonfly2/issues/984))
- when write last piece, generate digest ([#982](https://github.com/dragonflyoss/Dragonfly2/issues/982))

### Ffix
- typo in Makefile ([#975](https://github.com/dragonflyoss/Dragonfly2/issues/975))

### Fix
- scheduler config typo ([#983](https://github.com/dragonflyoss/Dragonfly2/issues/983))


<a name="v2.0.2-beta.0"></a>
## [v2.0.2-beta.0] - 2022-01-12
### Chore
- change docker.pkg.github.com to ghcr.io ([#973](https://github.com/dragonflyoss/Dragonfly2/issues/973))

### Feat
- merge same tasks in daemon ([#977](https://github.com/dragonflyoss/Dragonfly2/issues/977))
- if cdn is deleted, clear cdn related information ([#967](https://github.com/dragonflyoss/Dragonfly2/issues/967))

### Test
- dump goroutine in e2e ([#980](https://github.com/dragonflyoss/Dragonfly2/issues/980))


<a name="v2.0.2-alpha.8"></a>
## [v2.0.2-alpha.8] - 2022-01-04
### Chore
- change docker.pkg.github.com to ghcr.io


<a name="v2.0.2-alpha.7"></a>
## [v2.0.2-alpha.7] - 2021-12-31
### Chore
- copy e2e proxy log to artifact ([#962](https://github.com/dragonflyoss/Dragonfly2/issues/962))
- add version metric ([#954](https://github.com/dragonflyoss/Dragonfly2/issues/954))
- optimize back source update digest logic ([#950](https://github.com/dragonflyoss/Dragonfly2/issues/950))

### Docs
- update plugin doc ([#951](https://github.com/dragonflyoss/Dragonfly2/issues/951))

### Feat
- add default DiskGCThresholdPercent and ignore it when is 0 ([#971](https://github.com/dragonflyoss/Dragonfly2/issues/971))
- improve redirect to allow url rewrite ([#969](https://github.com/dragonflyoss/Dragonfly2/issues/969))
- Add useProxies to registryMirror allowing to mirror more anything ([#965](https://github.com/dragonflyoss/Dragonfly2/issues/965))
- change metrics port to 8000 ([#964](https://github.com/dragonflyoss/Dragonfly2/issues/964))
- add daemon metrics support ([#960](https://github.com/dragonflyoss/Dragonfly2/issues/960))
- support disk usage gc in client ([#953](https://github.com/dragonflyoss/Dragonfly2/issues/953))
- update source.Response and source client interface ([#945](https://github.com/dragonflyoss/Dragonfly2/issues/945))
- remove stat log from scheduler ([#946](https://github.com/dragonflyoss/Dragonfly2/issues/946))
- support recursive download in dfget ([#932](https://github.com/dragonflyoss/Dragonfly2/issues/932))
- add kmutex and krwmutex ([#934](https://github.com/dragonflyoss/Dragonfly2/issues/934))

### Fix
- schedulers send invalid direct piece ([#970](https://github.com/dragonflyoss/Dragonfly2/issues/970))
- use 'parent' as mainPeer in PeerPacket in removePeerFromCurrentTree() ([#957](https://github.com/dragonflyoss/Dragonfly2/issues/957))
- size scope empty ([#941](https://github.com/dragonflyoss/Dragonfly2/issues/941))
- not handle base.Code_SchedTaskStatusError in client ([#938](https://github.com/dragonflyoss/Dragonfly2/issues/938))
- infinitely get pieces when piece num is invalid ([#926](https://github.com/dragonflyoss/Dragonfly2/issues/926))


<a name="v2.0.2-alpha.6"></a>
## [v2.0.2-alpha.6] - 2021-12-15
### Feat
- make idgen package public ([#931](https://github.com/dragonflyoss/Dragonfly2/issues/931))
- make dfpath public ([#929](https://github.com/dragonflyoss/Dragonfly2/issues/929))

### Refactor
- scheduler register task ([#924](https://github.com/dragonflyoss/Dragonfly2/issues/924))


<a name="v2.0.2-alpha.5"></a>
## [v2.0.2-alpha.5] - 2021-12-13
### Docs
- update plugin docs ([#921](https://github.com/dragonflyoss/Dragonfly2/issues/921))

### Fix
- plugin dir is empty ([#922](https://github.com/dragonflyoss/Dragonfly2/issues/922))


<a name="v2.0.2-alpha.4"></a>
## [v2.0.2-alpha.4] - 2021-12-13
### Feat
- dfdaemon list scheduler cluster with multi idc ([#917](https://github.com/dragonflyoss/Dragonfly2/issues/917))
- update submodule ([#916](https://github.com/dragonflyoss/Dragonfly2/issues/916))
- update task access time ([#909](https://github.com/dragonflyoss/Dragonfly2/issues/909))
- optmize dfget package upgrade support ([#804](https://github.com/dragonflyoss/Dragonfly2/issues/804))
- support create container without docker-compose ([#915](https://github.com/dragonflyoss/Dragonfly2/issues/915))

### Fix
- peer gc ([#918](https://github.com/dragonflyoss/Dragonfly2/issues/918))
- go plugin test build error ([#912](https://github.com/dragonflyoss/Dragonfly2/issues/912))
- typo ([#911](https://github.com/dragonflyoss/Dragonfly2/issues/911))

### Refactor
- move from io/ioutil to io and os packages ([#906](https://github.com/dragonflyoss/Dragonfly2/issues/906))

### Test
- idgen peer id ([#913](https://github.com/dragonflyoss/Dragonfly2/issues/913))


<a name="v2.0.2-alpha.3"></a>
## [v2.0.2-alpha.3] - 2021-12-09
### Feat
- add data directory ([#910](https://github.com/dragonflyoss/Dragonfly2/issues/910))

### Fix
- total pieces not set when back source ([#908](https://github.com/dragonflyoss/Dragonfly2/issues/908))


<a name="v2.0.2-alpha.2"></a>
## [v2.0.2-alpha.2] - 2021-12-09
### Chore
- support multi daemons e2e test ([#896](https://github.com/dragonflyoss/Dragonfly2/issues/896))

### Docs
- dir path ([#904](https://github.com/dragonflyoss/Dragonfly2/issues/904))

### Feat
- add data storage directory  ([#907](https://github.com/dragonflyoss/Dragonfly2/issues/907))

### Fix
- mismatch digest peer task did not mark invalid ([#903](https://github.com/dragonflyoss/Dragonfly2/issues/903))


<a name="v2.0.2-alpha.1"></a>
## [v2.0.2-alpha.1] - 2021-12-08
### Fix
- dfget dfpath ([#901](https://github.com/dragonflyoss/Dragonfly2/issues/901))


<a name="v2.0.2-alpha.0"></a>
## [v2.0.2-alpha.0] - 2021-12-08
### Chore
- update UnknownSourceFileLen ([#888](https://github.com/dragonflyoss/Dragonfly2/issues/888))
- update changelog
- upgrade to golang 1.17 and alpine 3.14 ([#861](https://github.com/dragonflyoss/Dragonfly2/issues/861))

### Docs
- add plugin guide ([#875](https://github.com/dragonflyoss/Dragonfly2/issues/875))
- keep alive ([#868](https://github.com/dragonflyoss/Dragonfly2/issues/868))
- **zh-CN:** refactor machine translation ([#783](https://github.com/dragonflyoss/Dragonfly2/issues/783))

### Feat
- dfdaemon update content length ([#895](https://github.com/dragonflyoss/Dragonfly2/issues/895))
- lint sh ([#876](https://github.com/dragonflyoss/Dragonfly2/issues/876))
- update helm charts ([#870](https://github.com/dragonflyoss/Dragonfly2/issues/870))
- update version to v2.0.1 ([#869](https://github.com/dragonflyoss/Dragonfly2/issues/869))
- add oauth timeout ([#867](https://github.com/dragonflyoss/Dragonfly2/issues/867))
- support customize transport in daemon ([#866](https://github.com/dragonflyoss/Dragonfly2/issues/866))
- console ([#865](https://github.com/dragonflyoss/Dragonfly2/issues/865))
- move dfnet to internal ([#862](https://github.com/dragonflyoss/Dragonfly2/issues/862))
- remove ifaceutils pkg ([#860](https://github.com/dragonflyoss/Dragonfly2/issues/860))
- move syncmap pkg([#859](https://github.com/dragonflyoss/Dragonfly2/issues/859))
- oauth interface auth ([#857](https://github.com/dragonflyoss/Dragonfly2/issues/857))

### Fix
- scheduler success event ([#891](https://github.com/dragonflyoss/Dragonfly2/issues/891))
- add cdn cluster to scheduler cluster ([#887](https://github.com/dragonflyoss/Dragonfly2/issues/887))
- small size task failed due to digest error ([#886](https://github.com/dragonflyoss/Dragonfly2/issues/886))
- searcher log ([#878](https://github.com/dragonflyoss/Dragonfly2/issues/878))
- error log ([#863](https://github.com/dragonflyoss/Dragonfly2/issues/863))

### Refactor
- dfpath pkg ([#879](https://github.com/dragonflyoss/Dragonfly2/issues/879))


<a name="v2.0.1-rc.7"></a>
## [v2.0.1-rc.7] - 2021-12-02
### Docs
- update quick-start.md format ([#850](https://github.com/dragonflyoss/Dragonfly2/issues/850))

### Feat
- add scopes validation ([#856](https://github.com/dragonflyoss/Dragonfly2/issues/856))
- log ([#852](https://github.com/dragonflyoss/Dragonfly2/issues/852))

### Fix
- file peer task back source digest not match ([#849](https://github.com/dragonflyoss/Dragonfly2/issues/849))


<a name="v2.0.1-rc.6"></a>
## [v2.0.1-rc.6] - 2021-12-01
### Feat
- get scheduler list with advertise ip ([#848](https://github.com/dragonflyoss/Dragonfly2/issues/848))


<a name="v2.0.1-rc.5"></a>
## [v2.0.1-rc.5] - 2021-12-01
### Feat
- support mutli manager addrs ([#846](https://github.com/dragonflyoss/Dragonfly2/issues/846))


<a name="v2.0.1-rc.4"></a>
## [v2.0.1-rc.4] - 2021-12-01
### Feat
- searcher plugin change return params ([#844](https://github.com/dragonflyoss/Dragonfly2/issues/844))


<a name="v2.0.1-rc.3"></a>
## [v2.0.1-rc.3] - 2021-12-01
### Feat
- searcher plugin change return params


<a name="v2.0.1-rc.2"></a>
## [v2.0.1-rc.2] - 2021-12-01
### Feat
- plugin log ([#843](https://github.com/dragonflyoss/Dragonfly2/issues/843))


<a name="v2.0.1-rc.1"></a>
## [v2.0.1-rc.1] - 2021-11-30
### Feat
- export searcher evaluate func ([#842](https://github.com/dragonflyoss/Dragonfly2/issues/842))
- add context for FindSchedulerCluster ([#841](https://github.com/dragonflyoss/Dragonfly2/issues/841))
- add application cdn clusters field ([#840](https://github.com/dragonflyoss/Dragonfly2/issues/840))


<a name="v2.0.1-rc.0"></a>
## [v2.0.1-rc.0] - 2021-11-30
### Feat
- update console submodule ([#838](https://github.com/dragonflyoss/Dragonfly2/issues/838))


<a name="v2.0.1-beta.6"></a>
## [v2.0.1-beta.6] - 2021-11-29
### Chore
- unify binary directory ([#828](https://github.com/dragonflyoss/Dragonfly2/issues/828))

### Feat
- preheat compatible with harbor ([#837](https://github.com/dragonflyoss/Dragonfly2/issues/837))
- gin version ([#833](https://github.com/dragonflyoss/Dragonfly2/issues/833))
- update manager image ([#831](https://github.com/dragonflyoss/Dragonfly2/issues/831))
- update helm charts version ([#824](https://github.com/dragonflyoss/Dragonfly2/issues/824))


<a name="v2.0.1-beta.5"></a>
## [v2.0.1-beta.5] - 2021-11-24
### Docs
- metrics configuration ([#816](https://github.com/dragonflyoss/Dragonfly2/issues/816))

### Feat
- add package reachable ([#822](https://github.com/dragonflyoss/Dragonfly2/issues/822))
- support list plugin ([#819](https://github.com/dragonflyoss/Dragonfly2/issues/819))
- scheduler and cdn report fqdn to manager ([#818](https://github.com/dragonflyoss/Dragonfly2/issues/818))


<a name="v2.0.1-beta.4"></a>
## [v2.0.1-beta.4] - 2021-11-22
### Docs
- manager apis ([#814](https://github.com/dragonflyoss/Dragonfly2/issues/814))

### Feat
- dfdaemon get scheduler list dynamically from manager ([#812](https://github.com/dragonflyoss/Dragonfly2/issues/812))

### Fix
- source plugin not loaded ([#811](https://github.com/dragonflyoss/Dragonfly2/issues/811))


<a name="v2.0.1-beta.3"></a>
## [v2.0.1-beta.3] - 2021-11-19
### Feat
- update image-spec version ([#808](https://github.com/dragonflyoss/Dragonfly2/issues/808))
- add security rule ([#806](https://github.com/dragonflyoss/Dragonfly2/issues/806))
- add idgen peer id ([#800](https://github.com/dragonflyoss/Dragonfly2/issues/800))

### Fix
- manager typo and cdn peer id ([#809](https://github.com/dragonflyoss/Dragonfly2/issues/809))

### Refactor
- scheduler evaluator ([#805](https://github.com/dragonflyoss/Dragonfly2/issues/805))


<a name="v2.0.1-beta.2"></a>
## [v2.0.1-beta.2] - 2021-11-15
### Chore
- add lint errcheck  and fix errcheck([#766](https://github.com/dragonflyoss/Dragonfly2/issues/766))
- optimize client storage gc log ([#790](https://github.com/dragonflyoss/Dragonfly2/issues/790))

### Feat
- optimize scheduler peer stat log ([#798](https://github.com/dragonflyoss/Dragonfly2/issues/798))
- replace sortedList with sortedUniqueList ([#793](https://github.com/dragonflyoss/Dragonfly2/issues/793))

### Test
- preheat image ([#794](https://github.com/dragonflyoss/Dragonfly2/issues/794))


<a name="v2.0.1-beta.1"></a>
## [v2.0.1-beta.1] - 2021-11-10
### Feat
- calculate piece metadata digest ([#787](https://github.com/dragonflyoss/Dragonfly2/issues/787))


<a name="v2.0.1-alpha.10"></a>
## [v2.0.1-alpha.10] - 2021-11-09
### Feat
- preheat skip certificate validation ([#786](https://github.com/dragonflyoss/Dragonfly2/issues/786))


<a name="v2.0.1-alpha.9"></a>
## [v2.0.1-alpha.9] - 2021-11-09
### Chore
- optimize client log
- add markdown lint ([#779](https://github.com/dragonflyoss/Dragonfly2/issues/779))
- update golang import lint ([#780](https://github.com/dragonflyoss/Dragonfly2/issues/780))

### Docs
- manager api ([#774](https://github.com/dragonflyoss/Dragonfly2/issues/774))
- **zh:** add zh docs ([#777](https://github.com/dragonflyoss/Dragonfly2/issues/777))

### Feat
- calculate piece metadata digest
- support traffic metrics by peer host ([#776](https://github.com/dragonflyoss/Dragonfly2/issues/776))

### Fix
- cdn AdvertiseIP not used ([#782](https://github.com/dragonflyoss/Dragonfly2/issues/782))

### Test
- scheduler supervisor ([#742](https://github.com/dragonflyoss/Dragonfly2/issues/742))


<a name="v2.0.1-alpha.8"></a>
## [v2.0.1-alpha.8] - 2021-10-29
### Chore
- optimize stream peer task ([#763](https://github.com/dragonflyoss/Dragonfly2/issues/763))

### Feat
- support dump http content in client for debugging ([#770](https://github.com/dragonflyoss/Dragonfly2/issues/770))
- remove calculate total count service ([#772](https://github.com/dragonflyoss/Dragonfly2/issues/772))
- add user list interface ([#771](https://github.com/dragonflyoss/Dragonfly2/issues/771))
- clear hashcircler and maputils package ([#768](https://github.com/dragonflyoss/Dragonfly2/issues/768))


<a name="v2.0.1-alpha.7"></a>
## [v2.0.1-alpha.7] - 2021-10-28
### Fix
- add peer to task failed because InnerBucketMaxLength is small ([#765](https://github.com/dragonflyoss/Dragonfly2/issues/765))


<a name="v2.0.1-alpha.6"></a>
## [v2.0.1-alpha.6] - 2021-10-28
### Chore
- check empty registry mirror ([#761](https://github.com/dragonflyoss/Dragonfly2/issues/761))

### Feat
- add cdn task peers monitor log ([#764](https://github.com/dragonflyoss/Dragonfly2/issues/764))
- change config key name ([#759](https://github.com/dragonflyoss/Dragonfly2/issues/759))

### Fix
- back source weight ([#762](https://github.com/dragonflyoss/Dragonfly2/issues/762))


<a name="v2.0.1-alpha.5"></a>
## [v2.0.1-alpha.5] - 2021-10-27
### Feat
- scheduler channel blocking ([#756](https://github.com/dragonflyoss/Dragonfly2/issues/756))


<a name="v2.0.1-alpha.4"></a>
## [v2.0.1-alpha.4] - 2021-10-26
### Chore
- optimize span context for report ([#747](https://github.com/dragonflyoss/Dragonfly2/issues/747))

### Docs
- add maxConcurrency comment ([#755](https://github.com/dragonflyoss/Dragonfly2/issues/755))
- add troubleshooting guide ([#752](https://github.com/dragonflyoss/Dragonfly2/issues/752))
- add load limit ([#745](https://github.com/dragonflyoss/Dragonfly2/issues/745))
- **en:** upgrade docs ([#673](https://github.com/dragonflyoss/Dragonfly2/issues/673))
- **runtime:** upgrade containerd runtime ([#748](https://github.com/dragonflyoss/Dragonfly2/issues/748))

### Feat
- add jobs api ([#751](https://github.com/dragonflyoss/Dragonfly2/issues/751))
- add config ([#746](https://github.com/dragonflyoss/Dragonfly2/issues/746))
- add preheat otel ([#741](https://github.com/dragonflyoss/Dragonfly2/issues/741))

### Fix
- client load ([#753](https://github.com/dragonflyoss/Dragonfly2/issues/753))


<a name="v2.0.1-alpha.3"></a>
## [v2.0.1-alpha.3] - 2021-10-20
### Feat
- add job logger ([#740](https://github.com/dragonflyoss/Dragonfly2/issues/740))


<a name="v2.0.1-alpha.2"></a>
## [v2.0.1-alpha.2] - 2021-10-20
### Feat
- manager add grpc jaeger ([#738](https://github.com/dragonflyoss/Dragonfly2/issues/738))
- load limit ([#739](https://github.com/dragonflyoss/Dragonfly2/issues/739))
- preheat cluster ([#731](https://github.com/dragonflyoss/Dragonfly2/issues/731))
- nsswitch ([#737](https://github.com/dragonflyoss/Dragonfly2/issues/737))
- export e2e logs ([#732](https://github.com/dragonflyoss/Dragonfly2/issues/732))


<a name="v2.0.1-alpha.1"></a>
## [v2.0.1-alpha.1] - 2021-10-13
### Chore
- repository name
- change docker registry name ([#725](https://github.com/dragonflyoss/Dragonfly2/issues/725))
- update config example ([#721](https://github.com/dragonflyoss/Dragonfly2/issues/721))
- release image to docker.pkg.github.com ([#703](https://github.com/dragonflyoss/Dragonfly2/issues/703))

### Docs
- update kubernetes docs ([#714](https://github.com/dragonflyoss/Dragonfly2/issues/714))
- add apis and preheat ([#712](https://github.com/dragonflyoss/Dragonfly2/issues/712))
- update kubernetes docs ([#705](https://github.com/dragonflyoss/Dragonfly2/issues/705))

### Feat
- compatible with V1 preheat  ([#720](https://github.com/dragonflyoss/Dragonfly2/issues/720))
- add grpc metric and refactor grpc server ([#686](https://github.com/dragonflyoss/Dragonfly2/issues/686))

### Fix
- peer empty parent ([#724](https://github.com/dragonflyoss/Dragonfly2/issues/724))
- client panic ([#719](https://github.com/dragonflyoss/Dragonfly2/issues/719))
- client goroutine and fd leak ([#713](https://github.com/dragonflyoss/Dragonfly2/issues/713))


<a name="v2.0.1-alpha.0"></a>
## [v2.0.1-alpha.0] - 2021-09-29
### Chore
- workflows ignore paths ([#697](https://github.com/dragonflyoss/Dragonfly2/issues/697))
- remove skip-duplicate-actions ([#690](https://github.com/dragonflyoss/Dragonfly2/issues/690))
- e2e workflows remove goproxy ([#677](https://github.com/dragonflyoss/Dragonfly2/issues/677))

### Docs
- scheduler config ([#698](https://github.com/dragonflyoss/Dragonfly2/issues/698))
- update kubernetes docs ([#696](https://github.com/dragonflyoss/Dragonfly2/issues/696))

### Feat
- add manager client list scheduler interface ([#694](https://github.com/dragonflyoss/Dragonfly2/issues/694))

### Fix
- skip check DisableAutoBackSource option when scheduler says back source ([#693](https://github.com/dragonflyoss/Dragonfly2/issues/693))

### Refactor
- scheduler supervisor ([#655](https://github.com/dragonflyoss/Dragonfly2/issues/655))


<a name="v2.0.1-a-rc2"></a>
## [v2.0.1-a-rc2] - 2021-09-23
### Chore
- export set log level ([#646](https://github.com/dragonflyoss/Dragonfly2/issues/646))
- enable calculate digest ([#656](https://github.com/dragonflyoss/Dragonfly2/issues/656))
- update build package config ([#653](https://github.com/dragonflyoss/Dragonfly2/issues/653))
- optimize advertise ip ([#652](https://github.com/dragonflyoss/Dragonfly2/issues/652))
- change zzy987 maintainers email ([#649](https://github.com/dragonflyoss/Dragonfly2/issues/649))
- update version ([#647](https://github.com/dragonflyoss/Dragonfly2/issues/647))

### Docs
- scheduler config ([#654](https://github.com/dragonflyoss/Dragonfly2/issues/654))

### Feat
- release fd ([#668](https://github.com/dragonflyoss/Dragonfly2/issues/668))
- add otel trace ([#650](https://github.com/dragonflyoss/Dragonfly2/issues/650))
- disable prepared statement ([#648](https://github.com/dragonflyoss/Dragonfly2/issues/648))

### Fix
- go library cve ([#666](https://github.com/dragonflyoss/Dragonfly2/issues/666))


<a name="v2.0.1-a-rc1"></a>
## [v2.0.1-a-rc1] - 2021-09-13
### Chore
- export set up daemon logging
- export set log level
- add lucy-cl maintainer ([#645](https://github.com/dragonflyoss/Dragonfly2/issues/645))
- makefile typo


<a name="v2.0.0"></a>
## [v2.0.0] - 2021-09-09
### Chore
- compatibility with v2.0.0 test ([#639](https://github.com/dragonflyoss/Dragonfly2/issues/639))
- skip e2e ([#631](https://github.com/dragonflyoss/Dragonfly2/issues/631))
- rename cdnsystem to cdn ([#626](https://github.com/dragonflyoss/Dragonfly2/issues/626))
- skip workflows ([#624](https://github.com/dragonflyoss/Dragonfly2/issues/624))
- update changelog ([#622](https://github.com/dragonflyoss/Dragonfly2/issues/622))
- update submodule version ([#608](https://github.com/dragonflyoss/Dragonfly2/issues/608))
- optimize app and tracer log ([#607](https://github.com/dragonflyoss/Dragonfly2/issues/607))

### Docs
- maintainers ([#636](https://github.com/dragonflyoss/Dragonfly2/issues/636))
- test guide link ([#635](https://github.com/dragonflyoss/Dragonfly2/issues/635))
- add manager preview ([#634](https://github.com/dragonflyoss/Dragonfly2/issues/634))
- install ([#628](https://github.com/dragonflyoss/Dragonfly2/issues/628))
- update document ([#625](https://github.com/dragonflyoss/Dragonfly2/issues/625))
- update docs/zh-CN/config/dfget.yaml ([#623](https://github.com/dragonflyoss/Dragonfly2/issues/623))
- Update documents ([#595](https://github.com/dragonflyoss/Dragonfly2/issues/595))
- update runtime guide in helm deploy ([#612](https://github.com/dragonflyoss/Dragonfly2/issues/612))

### Feat
- update version ([#640](https://github.com/dragonflyoss/Dragonfly2/issues/640))
- changelog ([#638](https://github.com/dragonflyoss/Dragonfly2/issues/638))
- update console submodule ([#637](https://github.com/dragonflyoss/Dragonfly2/issues/637))
- update submodule ([#632](https://github.com/dragonflyoss/Dragonfly2/issues/632))
- beautify scheduler & CDN log ([#618](https://github.com/dragonflyoss/Dragonfly2/issues/618))
- Print version information when the system starts up ([#620](https://github.com/dragonflyoss/Dragonfly2/issues/620))
- add piece download timeout ([#621](https://github.com/dragonflyoss/Dragonfly2/issues/621))
- notice client back source when rescheduled parent reach max times ([#611](https://github.com/dragonflyoss/Dragonfly2/issues/611))
- avoid report peer result fail due to context cancel & add backsource tracer ([#606](https://github.com/dragonflyoss/Dragonfly2/issues/606))
- optimize cdn check free space ([#603](https://github.com/dragonflyoss/Dragonfly2/issues/603))

### Feature
- refresh proto file ([#615](https://github.com/dragonflyoss/Dragonfly2/issues/615))

### Fix
- return failed piece ([#619](https://github.com/dragonflyoss/Dragonfly2/issues/619))

### Test
- preheat e2e ([#627](https://github.com/dragonflyoss/Dragonfly2/issues/627))


<a name="v0.5.0"></a>
## [v0.5.0] - 2021-09-06
### Chore
- add compatibility test workflow ([#594](https://github.com/dragonflyoss/Dragonfly2/issues/594))

### Feat
- client back source ([#579](https://github.com/dragonflyoss/Dragonfly2/issues/579))
- enable manager user's features ([#598](https://github.com/dragonflyoss/Dragonfly2/issues/598))
- add sni proxy support ([#600](https://github.com/dragonflyoss/Dragonfly2/issues/600))
- compatibility e2e with matrix ([#599](https://github.com/dragonflyoss/Dragonfly2/issues/599))

### Fix
- use string slice for header ([#601](https://github.com/dragonflyoss/Dragonfly2/issues/601))
- preheat-e2e timeout ([#602](https://github.com/dragonflyoss/Dragonfly2/issues/602))


<a name="v0.4.0"></a>
## [v0.4.0] - 2021-09-02
### Chore
- add copyright ([#593](https://github.com/dragonflyoss/Dragonfly2/issues/593))

### Docs
- rbac swagger comment

### Feat
- change scheduler cluster query params ([#596](https://github.com/dragonflyoss/Dragonfly2/issues/596))
- add oauth2 signin ([#591](https://github.com/dragonflyoss/Dragonfly2/issues/591))
- update scheduler cluster query params ([#587](https://github.com/dragonflyoss/Dragonfly2/issues/587))
- add time out when register ([#588](https://github.com/dragonflyoss/Dragonfly2/issues/588))
- skip verify when back to source ([#586](https://github.com/dragonflyoss/Dragonfly2/issues/586))
- update charts submodule ([#583](https://github.com/dragonflyoss/Dragonfly2/issues/583))
- support limit from dfget client ([#578](https://github.com/dragonflyoss/Dragonfly2/issues/578))

### Refactor
- rbac
- user interface

### Test
- print merge commit ([#581](https://github.com/dragonflyoss/Dragonfly2/issues/581))


<a name="v0.3.0"></a>
## [v0.3.0] - 2021-08-25
### Feat
- add cdn cluster id for scheduler cluster ([#580](https://github.com/dragonflyoss/Dragonfly2/issues/580))
- start process ([#572](https://github.com/dragonflyoss/Dragonfly2/issues/572))
- gin log to file ([#574](https://github.com/dragonflyoss/Dragonfly2/issues/574))
- add manager cors middleware ([#573](https://github.com/dragonflyoss/Dragonfly2/issues/573))
- change rabc code struct ([#552](https://github.com/dragonflyoss/Dragonfly2/issues/552))

### Fix
- use getTask instead of taskStore.Get, for the error cause type ([#571](https://github.com/dragonflyoss/Dragonfly2/issues/571))


<a name="v0.2.0"></a>
## [v0.2.0] - 2021-08-20
### Chore
- rename cdn server package to rpcserver ([#554](https://github.com/dragonflyoss/Dragonfly2/issues/554))
- optimize peer task report function ([#543](https://github.com/dragonflyoss/Dragonfly2/issues/543))
- optimize client rpc package name and other docs ([#541](https://github.com/dragonflyoss/Dragonfly2/issues/541))
- optimize grpc interceptor code ([#536](https://github.com/dragonflyoss/Dragonfly2/issues/536))

### Feat
- empty scheduler job ([#565](https://github.com/dragonflyoss/Dragonfly2/issues/565))
- optimize manager startup process ([#562](https://github.com/dragonflyoss/Dragonfly2/issues/562))
- update git submodule ([#560](https://github.com/dragonflyoss/Dragonfly2/issues/560))
- optimize scheduler start server ([#558](https://github.com/dragonflyoss/Dragonfly2/issues/558))
- add console ([#559](https://github.com/dragonflyoss/Dragonfly2/issues/559))
- generate swagger api ([#557](https://github.com/dragonflyoss/Dragonfly2/issues/557))
- add console submodule ([#549](https://github.com/dragonflyoss/Dragonfly2/issues/549))
- optimize get permission name ([#548](https://github.com/dragonflyoss/Dragonfly2/issues/548))
- rename task to job ([#544](https://github.com/dragonflyoss/Dragonfly2/issues/544))
- Add distribute Schedule Tracer & Refactor scheduler ([#537](https://github.com/dragonflyoss/Dragonfly2/issues/537))
- add artifacthub badge ([#524](https://github.com/dragonflyoss/Dragonfly2/issues/524))

### Feature
- update helm charts submodule ([#567](https://github.com/dragonflyoss/Dragonfly2/issues/567))
- Add manager charts with submodule ([#525](https://github.com/dragonflyoss/Dragonfly2/issues/525))

### Feature
- optimize manager project layout ([#540](https://github.com/dragonflyoss/Dragonfly2/issues/540))

### Fix
- adjust dfget download log ([#564](https://github.com/dragonflyoss/Dragonfly2/issues/564))
- wait available peer packet panic ([#561](https://github.com/dragonflyoss/Dragonfly2/issues/561))
- wrong content length in proxy
- cdn back source range size overflow ([#550](https://github.com/dragonflyoss/Dragonfly2/issues/550))

### Test
- compare image commit ([#538](https://github.com/dragonflyoss/Dragonfly2/issues/538))


<a name="v0.1.0"></a>
## v0.1.0 - 2021-08-12
### Chore
- optimize compute piece size function ([#528](https://github.com/dragonflyoss/Dragonfly2/issues/528))
- release workflow add checkout submodules
- add workflow docker build context
- workflows checkout with submodules
- docker with submodules
- helm install with dependency
- helm charts
- add charts submodule
- set GOPROXY with default value ([#463](https://github.com/dragonflyoss/Dragonfly2/issues/463))
- custom charts template namespace ([#416](https://github.com/dragonflyoss/Dragonfly2/issues/416))
- remove goreleaser go generate ([#409](https://github.com/dragonflyoss/Dragonfly2/issues/409))
- rename dfdaemon docker image ([#405](https://github.com/dragonflyoss/Dragonfly2/issues/405))
- remove macos ci ([#404](https://github.com/dragonflyoss/Dragonfly2/issues/404))
- add docs for dragonfly2.0 ([#234](https://github.com/dragonflyoss/Dragonfly2/issues/234))
- change bash to sh ([#383](https://github.com/dragonflyoss/Dragonfly2/issues/383))
- remove protoc.sh ([#341](https://github.com/dragonflyoss/Dragonfly2/issues/341))
- update CI timeout ([#328](https://github.com/dragonflyoss/Dragonfly2/issues/328))
- remove build script's git operation ([#321](https://github.com/dragonflyoss/Dragonfly2/issues/321))
- docker building workflow ([#323](https://github.com/dragonflyoss/Dragonfly2/issues/323))
- remove manager netcat-openbsd ([#298](https://github.com/dragonflyoss/Dragonfly2/issues/298))
- workflows remove main-rc branch ([#221](https://github.com/dragonflyoss/Dragonfly2/issues/221))
- change manager swagger docs path and add makefile swagger command ([#183](https://github.com/dragonflyoss/Dragonfly2/issues/183))
- add SECURITY.md ([#181](https://github.com/dragonflyoss/Dragonfly2/issues/181))
- change codeowners ([#179](https://github.com/dragonflyoss/Dragonfly2/issues/179))
- change codeowners to dragonfly2's maintainers and reviewers ([#169](https://github.com/dragonflyoss/Dragonfly2/issues/169))
- create custom issue template ([#168](https://github.com/dragonflyoss/Dragonfly2/issues/168))
- add pull request and issue templates ([#154](https://github.com/dragonflyoss/Dragonfly2/issues/154))

### Daemon
- add add additional peer id for some logs ([#205](https://github.com/dragonflyoss/Dragonfly2/issues/205))
- create output parent directory if not exists ([#188](https://github.com/dragonflyoss/Dragonfly2/issues/188))
- update default timeout and add context for downloading piece ([#190](https://github.com/dragonflyoss/Dragonfly2/issues/190))
- record failed code when unfinished and event for scheduler ([#176](https://github.com/dragonflyoss/Dragonfly2/issues/176))

### Docs
- install with an existing manager
- helm install
- helm install
- helm install
- Add dfget man page ([#388](https://github.com/dragonflyoss/Dragonfly2/issues/388))
- update v0.1.0-beta changelog ([#387](https://github.com/dragonflyoss/Dragonfly2/issues/387))
- add CHANGELOG.md
- add CODE_OF_CONDUCT.md ([#163](https://github.com/dragonflyoss/Dragonfly2/issues/163))

### Feat
- sub module
- sub project
- init id
- stop task
- select with cluster id
- manager grpc
- sub project
- update cdn host ([#530](https://github.com/dragonflyoss/Dragonfly2/issues/530))
- scheduler dynconfig expire time
- subproject commit
- subproject commit
- submodule
- sub project commit
- use cdn ip
- manager
- chart values
- file image
- kind load manager
- charts submodules
- back source when no available peers or scheduler error ([#521](https://github.com/dragonflyoss/Dragonfly2/issues/521))
- add task manager ([#490](https://github.com/dragonflyoss/Dragonfly2/issues/490))
- rename manager grpc ([#510](https://github.com/dragonflyoss/Dragonfly2/issues/510))
- Add stress testing tool for daemon ([#506](https://github.com/dragonflyoss/Dragonfly2/issues/506))
- scheduler getevaluator lock ([#502](https://github.com/dragonflyoss/Dragonfly2/issues/502))
- rename search file to searcher ([#484](https://github.com/dragonflyoss/Dragonfly2/issues/484))
- Add schedule log ([#495](https://github.com/dragonflyoss/Dragonfly2/issues/495))
- Extract peer event processing function ([#489](https://github.com/dragonflyoss/Dragonfly2/issues/489))
- optimize scheduler dynconfig ([#480](https://github.com/dragonflyoss/Dragonfly2/issues/480))
- optimize jwt ([#476](https://github.com/dragonflyoss/Dragonfly2/issues/476))
- register service to manager ([#475](https://github.com/dragonflyoss/Dragonfly2/issues/475))
- add searcher to scheduler cluster ([#462](https://github.com/dragonflyoss/Dragonfly2/issues/462))
- CDN implementation supports HDFS type storage ([#420](https://github.com/dragonflyoss/Dragonfly2/issues/420))
- add is_default to scheduler_cluster table ([#458](https://github.com/dragonflyoss/Dragonfly2/issues/458))
- add host info for scheduler and cdn ([#457](https://github.com/dragonflyoss/Dragonfly2/issues/457))
- Install e2e script ([#451](https://github.com/dragonflyoss/Dragonfly2/issues/451))
- Manager user logic ([#419](https://github.com/dragonflyoss/Dragonfly2/issues/419))
- Add plugin support for resource ([#291](https://github.com/dragonflyoss/Dragonfly2/issues/291))
- changelog ([#326](https://github.com/dragonflyoss/Dragonfly2/issues/326))
- remove queue package ([#275](https://github.com/dragonflyoss/Dragonfly2/issues/275))
- add ci badge ([#265](https://github.com/dragonflyoss/Dragonfly2/issues/265))
- remove slidingwindow and assertutils package ([#263](https://github.com/dragonflyoss/Dragonfly2/issues/263))

### Feature
- support mysql 5.6 ([#520](https://github.com/dragonflyoss/Dragonfly2/issues/520))
- support customize base image ([#519](https://github.com/dragonflyoss/Dragonfly2/issues/519))
- add kustomize yaml for deploying ([#349](https://github.com/dragonflyoss/Dragonfly2/issues/349))
- support basic auth for proxy ([#250](https://github.com/dragonflyoss/Dragonfly2/issues/250))
- add disk quota gc for daemon ([#215](https://github.com/dragonflyoss/Dragonfly2/issues/215))

### Feature
- enable grpc tracing ([#531](https://github.com/dragonflyoss/Dragonfly2/issues/531))
- remove proto redundant field ([#508](https://github.com/dragonflyoss/Dragonfly2/issues/508))
- update multiple registries support docs ([#481](https://github.com/dragonflyoss/Dragonfly2/issues/481))
- add multiple registry mirrors support ([#479](https://github.com/dragonflyoss/Dragonfly2/issues/479))
- disable proxy when config is empty ([#455](https://github.com/dragonflyoss/Dragonfly2/issues/455))
- add pod labels in helm chart ([#447](https://github.com/dragonflyoss/Dragonfly2/issues/447))
- optimize failed reason not set ([#446](https://github.com/dragonflyoss/Dragonfly2/issues/446))
- report peer result when failed to register ([#433](https://github.com/dragonflyoss/Dragonfly2/issues/433))
- rename PeerHost to Daemon in client ([#438](https://github.com/dragonflyoss/Dragonfly2/issues/438))
- move internal/rpc to pkg/rpc ([#436](https://github.com/dragonflyoss/Dragonfly2/issues/436))
- export peer.TaskManager for embedding dragonfly in custom binary ([#434](https://github.com/dragonflyoss/Dragonfly2/issues/434))
- optimize error message for proxy ([#428](https://github.com/dragonflyoss/Dragonfly2/issues/428))
- minimize daemon runtime capabilities ([#421](https://github.com/dragonflyoss/Dragonfly2/issues/421))
- add default filter in proxy for deployment and docs ([#417](https://github.com/dragonflyoss/Dragonfly2/issues/417))
- add jaeger for helm deployment ([#415](https://github.com/dragonflyoss/Dragonfly2/issues/415))
- update dfdaemon proxy port comment
- update cdn init container template ([#399](https://github.com/dragonflyoss/Dragonfly2/issues/399))
- update client config to Camel-Case format ([#393](https://github.com/dragonflyoss/Dragonfly2/issues/393))
- update helm charts deploy guide ([#386](https://github.com/dragonflyoss/Dragonfly2/issues/386))
- update helm charts ([#385](https://github.com/dragonflyoss/Dragonfly2/issues/385))
- support setns in client ([#378](https://github.com/dragonflyoss/Dragonfly2/issues/378))
- disable resolver server config ([#314](https://github.com/dragonflyoss/Dragonfly2/issues/314))
- update docs ([#307](https://github.com/dragonflyoss/Dragonfly2/issues/307))
- remove unsafe code in client/daemon/storage ([#258](https://github.com/dragonflyoss/Dragonfly2/issues/258))
- remove redundant configurations ([#216](https://github.com/dragonflyoss/Dragonfly2/issues/216))

### Fix
- proxy for stress testing tool ([#507](https://github.com/dragonflyoss/Dragonfly2/issues/507))
- add process level for scheduler peer task status ([#435](https://github.com/dragonflyoss/Dragonfly2/issues/435))
- infinite recursion in MkDirAll ([#358](https://github.com/dragonflyoss/Dragonfly2/issues/358))
- use atomic to avoid data race in client ([#254](https://github.com/dragonflyoss/Dragonfly2/issues/254))

### Fix
- scheduler concurrent dead lock ([#509](https://github.com/dragonflyoss/Dragonfly2/issues/509))
- scheduler pick candidate and associate child  encounter  dead lock ([#500](https://github.com/dragonflyoss/Dragonfly2/issues/500))
- generate proto file ([#483](https://github.com/dragonflyoss/Dragonfly2/issues/483))
- address typo ([#468](https://github.com/dragonflyoss/Dragonfly2/issues/468))
- dead lock when pt.failedPieceCh is full ([#466](https://github.com/dragonflyoss/Dragonfly2/issues/466))
- user table typo ([#453](https://github.com/dragonflyoss/Dragonfly2/issues/453))
- log specification ([#452](https://github.com/dragonflyoss/Dragonfly2/issues/452))
- wrong cache header ([#423](https://github.com/dragonflyoss/Dragonfly2/issues/423))
- close net namespace fd ([#418](https://github.com/dragonflyoss/Dragonfly2/issues/418))
- update static cdn config
- wrong daemon config and kubectl image tag ([#398](https://github.com/dragonflyoss/Dragonfly2/issues/398))
- update mapsturcture decode and remove unused config ([#396](https://github.com/dragonflyoss/Dragonfly2/issues/396))
- update DynconfigOptions typo ([#390](https://github.com/dragonflyoss/Dragonfly2/issues/390))
- gc test ([#370](https://github.com/dragonflyoss/Dragonfly2/issues/370))
- scheduler panic ([#356](https://github.com/dragonflyoss/Dragonfly2/issues/356))
- use seederName to replace the PeerID to generate the UUID ([#355](https://github.com/dragonflyoss/Dragonfly2/issues/355))
- check health too long when dfdaemon is unavailable ([#344](https://github.com/dragonflyoss/Dragonfly2/issues/344))
- cfgFile nil error ([#224](https://github.com/dragonflyoss/Dragonfly2/issues/224))
- when load config from cdn directory in dynconfig, skip sub directories ([#310](https://github.com/dragonflyoss/Dragonfly2/issues/310))
- Makefile and build.sh ([#309](https://github.com/dragonflyoss/Dragonfly2/issues/309))
- ci badge ([#281](https://github.com/dragonflyoss/Dragonfly2/issues/281))
- change peerPacketReady to buffer channel ([#256](https://github.com/dragonflyoss/Dragonfly2/issues/256))
- cdn gc dead lock ([#231](https://github.com/dragonflyoss/Dragonfly2/issues/231))
- change manager docs path ([#193](https://github.com/dragonflyoss/Dragonfly2/issues/193))
- **manager:** modify to config from scheduler_config in swagger yaml ([#317](https://github.com/dragonflyoss/Dragonfly2/issues/317))

### Refactor
- manager server new instance ([#464](https://github.com/dragonflyoss/Dragonfly2/issues/464))
- update arch ([#319](https://github.com/dragonflyoss/Dragonfly2/issues/319))
- remove benchmark-rate and rename not-back-source ([#245](https://github.com/dragonflyoss/Dragonfly2/issues/245))
- support multi digest not only md5 ([#236](https://github.com/dragonflyoss/Dragonfly2/issues/236))
- simplify to make imports more format ([#230](https://github.com/dragonflyoss/Dragonfly2/issues/230))
- **manager:** modify mysql table schema, orm json tag. ([#283](https://github.com/dragonflyoss/Dragonfly2/issues/283))

### Test
- scheduler manager client
- E2E download concurrency ([#467](https://github.com/dragonflyoss/Dragonfly2/issues/467))
- E2E test use kind's containerd ([#448](https://github.com/dragonflyoss/Dragonfly2/issues/448))
- manager config ([#392](https://github.com/dragonflyoss/Dragonfly2/issues/392))
- idgen add digest ([#243](https://github.com/dragonflyoss/Dragonfly2/issues/243))


[Unreleased]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3...HEAD
[v2.0.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.9...v2.0.3
[v2.0.3-beta.9]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.8...v2.0.3-beta.9
[v2.0.3-beta.8]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.7...v2.0.3-beta.8
[v2.0.3-beta.7]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2...v2.0.3-beta.7
[v2.0.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.6...v2.0.2
[v2.0.3-beta.6]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.5...v2.0.3-beta.6
[v2.0.3-beta.5]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.4...v2.0.3-beta.5
[v2.0.3-beta.4]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-alpha.1...v2.0.3-beta.4
[v2.0.3-alpha.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.3...v2.0.3-alpha.1
[v2.0.3-beta.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.2...v2.0.3-beta.3
[v2.0.3-beta.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.1...v2.0.3-beta.2
[v2.0.3-beta.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-beta.0...v2.0.3-beta.1
[v2.0.3-beta.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.3-alpha.0...v2.0.3-beta.0
[v2.0.3-alpha.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.27...v2.0.3-alpha.0
[v2.0.2-rc.27]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.26...v2.0.2-rc.27
[v2.0.2-rc.26]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1...v2.0.2-rc.26
[v2.0.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.25...v2.0.1
[v2.0.2-rc.25]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.24...v2.0.2-rc.25
[v2.0.2-rc.24]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.23...v2.0.2-rc.24
[v2.0.2-rc.23]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.22...v2.0.2-rc.23
[v2.0.2-rc.22]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.21...v2.0.2-rc.22
[v2.0.2-rc.21]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.20...v2.0.2-rc.21
[v2.0.2-rc.20]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.19...v2.0.2-rc.20
[v2.0.2-rc.19]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.18...v2.0.2-rc.19
[v2.0.2-rc.18]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.17...v2.0.2-rc.18
[v2.0.2-rc.17]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.16...v2.0.2-rc.17
[v2.0.2-rc.16]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.15...v2.0.2-rc.16
[v2.0.2-rc.15]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.14...v2.0.2-rc.15
[v2.0.2-rc.14]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.13...v2.0.2-rc.14
[v2.0.2-rc.13]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.12...v2.0.2-rc.13
[v2.0.2-rc.12]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.11...v2.0.2-rc.12
[v2.0.2-rc.11]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.10...v2.0.2-rc.11
[v2.0.2-rc.10]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.9...v2.0.2-rc.10
[v2.0.2-rc.9]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.8...v2.0.2-rc.9
[v2.0.2-rc.8]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.7...v2.0.2-rc.8
[v2.0.2-rc.7]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.5...v2.0.2-rc.7
[v2.0.2-rc.5]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.6...v2.0.2-rc.5
[v2.0.2-rc.6]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.4...v2.0.2-rc.6
[v2.0.2-rc.4]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.3...v2.0.2-rc.4
[v2.0.2-rc.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.2...v2.0.2-rc.3
[v2.0.2-rc.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.1...v2.0.2-rc.2
[v2.0.2-rc.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-rc.0...v2.0.2-rc.1
[v2.0.2-rc.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-beta.6...v2.0.2-rc.0
[v2.0.2-beta.6]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-beta.5...v2.0.2-beta.6
[v2.0.2-beta.5]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-beta.4...v2.0.2-beta.5
[v2.0.2-beta.4]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-beta.3...v2.0.2-beta.4
[v2.0.2-beta.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-beta.2...v2.0.2-beta.3
[v2.0.2-beta.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-beta.1...v2.0.2-beta.2
[v2.0.2-beta.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-beta.0...v2.0.2-beta.1
[v2.0.2-beta.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.8...v2.0.2-beta.0
[v2.0.2-alpha.8]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.7...v2.0.2-alpha.8
[v2.0.2-alpha.7]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.6...v2.0.2-alpha.7
[v2.0.2-alpha.6]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.5...v2.0.2-alpha.6
[v2.0.2-alpha.5]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.4...v2.0.2-alpha.5
[v2.0.2-alpha.4]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.3...v2.0.2-alpha.4
[v2.0.2-alpha.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.2...v2.0.2-alpha.3
[v2.0.2-alpha.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.1...v2.0.2-alpha.2
[v2.0.2-alpha.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.2-alpha.0...v2.0.2-alpha.1
[v2.0.2-alpha.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.7...v2.0.2-alpha.0
[v2.0.1-rc.7]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.6...v2.0.1-rc.7
[v2.0.1-rc.6]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.5...v2.0.1-rc.6
[v2.0.1-rc.5]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.4...v2.0.1-rc.5
[v2.0.1-rc.4]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.3...v2.0.1-rc.4
[v2.0.1-rc.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.2...v2.0.1-rc.3
[v2.0.1-rc.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.1...v2.0.1-rc.2
[v2.0.1-rc.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.0...v2.0.1-rc.1
[v2.0.1-rc.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-beta.6...v2.0.1-rc.0
[v2.0.1-beta.6]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-beta.5...v2.0.1-beta.6
[v2.0.1-beta.5]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-beta.4...v2.0.1-beta.5
[v2.0.1-beta.4]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-beta.3...v2.0.1-beta.4
[v2.0.1-beta.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-beta.2...v2.0.1-beta.3
[v2.0.1-beta.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-beta.1...v2.0.1-beta.2
[v2.0.1-beta.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.10...v2.0.1-beta.1
[v2.0.1-alpha.10]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.9...v2.0.1-alpha.10
[v2.0.1-alpha.9]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.8...v2.0.1-alpha.9
[v2.0.1-alpha.8]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.7...v2.0.1-alpha.8
[v2.0.1-alpha.7]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.6...v2.0.1-alpha.7
[v2.0.1-alpha.6]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.5...v2.0.1-alpha.6
[v2.0.1-alpha.5]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.4...v2.0.1-alpha.5
[v2.0.1-alpha.4]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.3...v2.0.1-alpha.4
[v2.0.1-alpha.3]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.2...v2.0.1-alpha.3
[v2.0.1-alpha.2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.1...v2.0.1-alpha.2
[v2.0.1-alpha.1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-alpha.0...v2.0.1-alpha.1
[v2.0.1-alpha.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-a-rc2...v2.0.1-alpha.0
[v2.0.1-a-rc2]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-a-rc1...v2.0.1-a-rc2
[v2.0.1-a-rc1]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.0...v2.0.1-a-rc1
[v2.0.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.5.0...v2.0.0
[v0.5.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.1.0...v0.2.0
