<a name="unreleased"></a>
## [Unreleased]

### Chore
- upgrade to golang 1.17 and alpine 3.14 ([#861](https://github.com/dragonflyoss/Dragonfly2/issues/861))

### Docs
- **zh-CN:** refactor machine translation ([#783](https://github.com/dragonflyoss/Dragonfly2/issues/783))

### Feat
- move dfnet to internal ([#862](https://github.com/dragonflyoss/Dragonfly2/issues/862))
- remove ifaceutils pkg ([#860](https://github.com/dragonflyoss/Dragonfly2/issues/860))
- move syncmap pkg([#859](https://github.com/dragonflyoss/Dragonfly2/issues/859))

### Fix
- error log ([#863](https://github.com/dragonflyoss/Dragonfly2/issues/863))


<a name="v2.0.1-rc.8"></a>
## [v2.0.1-rc.8] - 2021-12-03
### Feat
- oauth interface auth ([#857](https://github.com/dragonflyoss/Dragonfly2/issues/857))


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
- update verison ([#640](https://github.com/dragonflyoss/Dragonfly2/issues/640))
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


[Unreleased]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.8...HEAD
[v2.0.1-rc.8]: https://github.com/dragonflyoss/Dragonfly2/compare/v2.0.1-rc.7...v2.0.1-rc.8
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
