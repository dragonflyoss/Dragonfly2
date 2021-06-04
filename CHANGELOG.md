<a name="unreleased"></a>
## [Unreleased]


<a name="v0.1.0-alpha"></a>
## v0.1.0-alpha - 2021-06-04
### Chore
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
- add CHANGELOG
- add CODE_OF_CONDUCT.md ([#163](https://github.com/dragonflyoss/Dragonfly2/issues/163))

### Feat
- remove queue package ([#275](https://github.com/dragonflyoss/Dragonfly2/issues/275))
- add ci badge ([#265](https://github.com/dragonflyoss/Dragonfly2/issues/265))
- remove slidingwindow and assertutils package ([#263](https://github.com/dragonflyoss/Dragonfly2/issues/263))

### Feature
- remove unsafe code in client/daemon/storage ([#258](https://github.com/dragonflyoss/Dragonfly2/issues/258))
- remove redundant configurations ([#216](https://github.com/dragonflyoss/Dragonfly2/issues/216))

### Feature
- support basic auth for proxy ([#250](https://github.com/dragonflyoss/Dragonfly2/issues/250))
- add disk quota gc for daemon ([#215](https://github.com/dragonflyoss/Dragonfly2/issues/215))

### Fix
- use atomic to avoid data race in client ([#254](https://github.com/dragonflyoss/Dragonfly2/issues/254))

### Fix
- ci badge ([#281](https://github.com/dragonflyoss/Dragonfly2/issues/281))
- change peerPacketReady to buffer channel ([#256](https://github.com/dragonflyoss/Dragonfly2/issues/256))
- cdn gc dead lock ([#231](https://github.com/dragonflyoss/Dragonfly2/issues/231))
- cfgFile nil error ([#224](https://github.com/dragonflyoss/Dragonfly2/issues/224))
- change manager docs path ([#193](https://github.com/dragonflyoss/Dragonfly2/issues/193))

### Refactor
- remove benchmark-rate and rename not-back-source ([#245](https://github.com/dragonflyoss/Dragonfly2/issues/245))
- support multi digest not only md5 ([#236](https://github.com/dragonflyoss/Dragonfly2/issues/236))
- simplify to make imports more format ([#230](https://github.com/dragonflyoss/Dragonfly2/issues/230))
- **manager:** modify mysql table schema, orm json tag. ([#283](https://github.com/dragonflyoss/Dragonfly2/issues/283))

### Test
- idgen add digest ([#243](https://github.com/dragonflyoss/Dragonfly2/issues/243))


[Unreleased]: https://github.com/dragonflyoss/Dragonfly2/compare/v0.1.0-alpha...HEAD
