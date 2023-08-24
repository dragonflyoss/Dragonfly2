# Dragonfly Project Governance

As a CNCF member project, Dragonfly project abides by the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## Overview

- [Maintainership](#Maintainership)
- [Adding Maintainers](#Adding-Maintainers)
- [Removal of Inactive Maintainers](#Removal-of-Inactive-Maintainers)
- [How to make decision](#How-to-make-decision)
- [Updating Governance](#Updating-Governance)

## Maintainership

Maintainers of Dragonfly share the responsibility. And they have 3 things in general:

- share responsibility in the project's success;
- make a long-term investment to improve the project;
- spend time doing whatever needs to, not only the most interesting part;

Maintainers are often working hard, but sometimes what they do is hardly appreciate.
It is easy to work on the fancy part or technically advanced feature. It is harder
to work on minor bugfix, small improvement, long-term stable optimization or
others. While all of the above is the essential parts to build up a successful project.

## Adding Maintainers

Maintainers are foremost and first contributors that have dedicated to the long
term success of the project. Contributors wishing to become maintainers are
expected to deeply involved in tackling issues, contributing codes, review
proposals and codes for more than two months.

Maintainership is built on trust. Trust is totally beyond just contributing
code. Thus it is definitely worthy to achieving current maintainers' trust via
bringing best interest for the project.

Current maintainers will hold a maintainer meeting periodically. During the
meeting, filtering a list of active contributors that have proven to invest
regular time on project over the prior months. From the list, if maintainers
find that one or more are qualified candidates, then a proposal of adding
maintainers can be submitted to GitHub via a pull request. If a vote of at
least 50% agree with the proposal, the newly added maintainer must be treated
valid.

## Removal of Inactive Maintainers

Similar to adding maintainers, existing maintainer can been removed from the
active maintainer list. If the existing maintainer meet one of the conditions
below, any other maintainers can proposed to remove him from active list via a
pull request:

- A maintainer has not participated in community activities for more than last
  three months;
- A maintainer did not obey to the governance rules more than twice;

After confirming conditions above, in theory maintainer can be removed from
list directly unless the original maintainer requests to remain maintainer seat
and gets a vote of at least 50% of other maintainers.

If a maintainer is removed from the maintaining list, other maintainers should
at a column to the alumni part to thank the contribution made from the inactive
maintainer.

## How to make decision

Dragonfly is an open-source project illustrating the concept of open. This
means that Dragonfly repository is the source of truth for every aspect of the
project, including raw value, design, doc, roadmap, interfaces and so on. If it
is one part of the project, it should be in the repo.

To be honest, all decisions can be regarded as an update to project.

Any updates or changes which take effect on Dragonfly, no matter big or small,
should follow three steps below:

- Step 1: Open a pull request;
- Step 2: Discuss under the pull request;
- Step 3: Merge or refuse the pull request.

When Dragonfly has less than seven maintainers, a pull request except adding
maintainers pull request could be merged, if it meets both conditions below:

- at least one maintainer commented `LGTM` to the pull request;
- no other maintainers have opposite opinion on it.

When Dragonfly has more than seven maintainers, a pull request except adding
maintainers pull request could be merged, if it meets conditions below:

- at least two maintainers commented `LGTM` to the pull request;

## Updating Governance

All substantive updates in Governance require a supermajority maintainers
vote.
