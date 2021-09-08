<a name="unreleased"></a>
## [Unreleased]

### Chore
- update submodule version ([#608](https://github.com/dragonflyoss/Dragonfly2/issues/608))
- optimize app and tracer log ([#607](https://github.com/dragonflyoss/Dragonfly2/issues/607))

### Docs
- update runtime guide in helm deploy ([#612](https://github.com/dragonflyoss/Dragonfly2/issues/612))

### Feat
- notice client back source when rescheduled parent reach max times ([#611](https://github.com/dragonflyoss/Dragonfly2/issues/611))

### Feature
- refresh proto file ([#615](https://github.com/dragonflyoss/Dragonfly2/issues/615))

### Fix
- return failed piece ([#619](https://github.com/dragonflyoss/Dragonfly2/issues/619))


<a name="v2.0.0-alpha-2"></a>
## [v2.0.0-alpha-2] - 2021-09-06
### Feat
- avoid report peer result fail due to context cancel & add backsource tracer ([#606](https://github.com/dragonflyoss/Dragonfly2/issues/606))
- optimize cdn check free space ([#603](https://github.com/dragonflyoss/Dragonfly2/issues/603))


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
## [v0.1.0] - 2021-08-12
### Chore
- optimize compute piece size function ([#528](https://github.com/dragonflyoss/Dragonfly2/issues/528))
- release workflow add checkout submodules
- add workflow docker build context
- workflows checkout with submodules
- docker with submodules
- helm install with dependency
- helm charts
- add charts submodule

### Docs
- install with an existing manager
- helm install
- helm install
- helm install

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

### Feature
- enable grpc tracing ([#531](https://github.com/dragonflyoss/Dragonfly2/issues/531))
- remove proto redundant field ([#508](https://github.com/dragonflyoss/Dragonfly2/issues/508))
- update multiple registries support docs ([#481](https://github.com/dragonflyoss/Dragonfly2/issues/481))
- add multiple registry mirrors support ([#479](https://github.com/dragonflyoss/Dragonfly2/issues/479))

### Feature
- support mysql 5.6 ([#520](https://github.com/dragonflyoss/Dragonfly2/issues/520))
- support customize base image ([#519](https://github.com/dragonflyoss/Dragonfly2/issues/519))

### Fix
- proxy for stress testing tool ([#507](https://github.com/dragonflyoss/Dragonfly2/issues/507))

### Fix
- scheduler concurrent dead lock ([#509](https://github.com/dragonflyoss/Dragonfly2/issues/509))
- scheduler pick candidate and associate child  encounter  dead lock ([#500](https://github.com/dragonflyoss/Dragonfly2/issues/500))
- generate proto file ([#483](https://github.com/dragonflyoss/Dragonfly2/issues/483))
- address typo ([#468](https://github.com/dragonflyoss/Dragonfly2/issues/468))
- dead lock when pt.failedPieceCh is full ([#466](https://github.com/dragonflyoss/Dragonfly2/issues/466))

### Test
- scheduler manager client


<a name="v2.0.0-alpha-inner-test"></a>
## [v2.0.0-alpha-inner-test] - 2021-07-20
### Chore
- set GOPROXY with default value ([#463](https://github.com/dragonflyoss/Dragonfly2/issues/463))

### Feat
- optimize jwt ([#476](https://github.com/dragonflyoss/Dragonfly2/issues/476))
- register service to manager ([#475](https://github.com/dragonflyoss/Dragonfly2/issues/475))
- add searcher to scheduler cluster ([#462](https://github.com/dragonflyoss/Dragonfly2/issues/462))
- CDN implementation supports HDFS type storage ([#420](https://github.com/dragonflyoss/Dragonfly2/issues/420))
- add is_default to scheduler_cluster table ([#458](https://github.com/dragonflyoss/Dragonfly2/issues/458))
- add host info for scheduler and cdn ([#457](https://github.com/dragonflyoss/Dragonfly2/issues/457))

### Feature
- add multiple registry mirrors support

### Refactor
- manager server new instance ([#464](https://github.com/dragonflyoss/Dragonfly2/issues/464))

### Test
- E2E download concurrency ([#467](https://github.com/dragonflyoss/Dragonfly2/issues/467))


<a name="v2.0.0-alpha"></a>
## [v2.0.0-alpha] - 2021-07-12
### Feat
- Install e2e script ([#451](https://github.com/dragonflyoss/Dragonfly2/issues/451))

### Feature
- disable proxy when config is empty ([#455](https://github.com/dragonflyoss/Dragonfly2/issues/455))

### Fix
- user table typo ([#453](https://github.com/dragonflyoss/Dragonfly2/issues/453))
- log specification ([#452](https://github.com/dragonflyoss/Dragonfly2/issues/452))


<a name="v0.1.0-beta-2"></a>
## v0.1.0-beta-2 - 2021-07-12
### Chore
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
- Add dfget man page ([#388](https://github.com/dragonflyoss/Dragonfly2/issues/388))
- update v0.1.0-beta changelog ([#387](https://github.com/dragonflyoss/Dragonfly2/issues/387))
- add CHANGELOG.md
- add CODE_OF_CONDUCT.md ([#163](https://github.com/dragonflyoss/Dragonfly2/issues/163))

### Feat
- Manager user logic ([#419](https://github.com/dragonflyoss/Dragonfly2/issues/419))
- Add plugin support for resource ([#291](https://github.com/dragonflyoss/Dragonfly2/issues/291))
- changelog ([#326](https://github.com/dragonflyoss/Dragonfly2/issues/326))
- remove queue package ([#275](https://github.com/dragonflyoss/Dragonfly2/issues/275))
- add ci badge ([#265](https://github.com/dragonflyoss/Dragonfly2/issues/265))
- remove slidingwindow and assertutils package ([#263](https://github.com/dragonflyoss/Dragonfly2/issues/263))

### Feature
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

### Feature
- add kustomize yaml for deploying ([#349](https://github.com/dragonflyoss/Dragonfly2/issues/349))
- support basic auth for proxy ([#250](https://github.com/dragonflyoss/Dragonfly2/issues/250))
- add disk quota gc for daemon ([#215](https://github.com/dragonflyoss/Dragonfly2/issues/215))

### Fix
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

### Fix
- add process level for scheduler peer task status ([#435](https://github.com/dragonflyoss/Dragonfly2/issues/435))
- infinite recursion in MkDirAll ([#358](https://github.com/dragonflyoss/Dragonfly2/issues/358))
- use atomic to avoid data race in client ([#254](https://github.com/dragonflyoss/Dragonfly2/issues/254))

### Refactor
- update arch ([#319](https://github.com/dragonflyoss/Dragonfly2/issues/319))
- remove benchmark-rate and rename not-back-source ([#245](https://github.com/dragonflyoss/Dragonfly2/issues/245))
- support multi digest not only md5 ([#236](https://github.com/dragonflyoss/Dragonfly2/issues/236))
- simplify to make imports more format ([#230](https://github.com/dragonflyoss/Dragonfly2/issues/230))
- **manager:** modify mysql table schema, orm json tag. ([#283](https://github.com/dragonflyoss/Dragonfly2/issues/283))

### Test
- E2E test use kind's containerd ([#448](https://github.com/dragonflyoss/Dragonfly2/issues/448))
- manager config ([#392](https://github.com/dragonflyoss/Dragonfly2/issues/392))
- idgen add digest ([#243](https://github.com/dragonflyoss/Dragonfly2/issues/243))


[Unreleased]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.0-alpha-2...HEAD
[v2.0.0-alpha-2]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.5.0...v2.0.0-alpha-2
[v0.5.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.0-alpha-inner-test...v0.1.0
[v2.0.0-alpha-inner-test]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.0-alpha...v2.0.0-alpha-inner-test
[v2.0.0-alpha]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.1.0-beta-2...v2.0.0-alpha
