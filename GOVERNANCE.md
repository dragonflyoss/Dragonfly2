# Dragonfly Project Governance

As a CNCF member project, Dragonfly project abides by the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

This doc outlines the responsibilities of contributor roles in Dragonfly. The Dragonfly project is subdivided into sub-projects
under (predominantly, but not exclusively) nydus, nydus-snapshotter, api, docs, console and client.
Responsibilities for roles are scoped to these sub-projects (repos).

We have six levels of responsibility including:

- Contributor
- Member
- Approver
- Maintainer
- Administrator

## Contributor

A Contributor is someone who has contributed to the project by submitting code, issues, or participating in discussions.

### Responsibilities

- Contribute code changes via pull requests (PRs).
- Raise issues for bugs, enhancements, or other discussions.
- Participate in code reviews if requested.
- Follow the project's Code of Conduct and contribution guidelines.

### Requirements

- Have a GitHub account.
- Sign the Contributor License Agreement (if applicable).
- Adhere to the project's coding standards and guidelines.Contributor

## Member

A Member is a contributor who has shown long-term commitment to the project,
consistently contributing high-quality work and actively participating in the community.

Responsibilities:

- All responsibilities of a Contributor.
- Participate in project planning and decision-making processes.
- Mentor new contributors.
- Actively participate in community meetings and discussions.

Requirements:

- Demonstrated history of contributions (code, reviews, discussions) over at least 3 months.
- Invitation by a Maintainer or Administrator based on consensus.
- Continued adherence to the project's Code of Conduct.

## Approver

An Approver is a member who has the additional responsibility of reviewing and approving PRs that affect the project.

Responsibilities:

- All responsibilities of a Member.
- Review and approve PRs from Contributors and Members.
- Ensure that changes adhere to coding standards, do not introduce bugs, and improve the project.
- Help triage issues and provide feedback on improvements.

Requirements:

- Demonstrated expertise in the project’s codebase.
- Consistent and high-quality contributions.
- Trusted by Maintainers and Administrators to provide accurate reviews and approvals.

## Maintainer

A Maintainer is an experienced and trusted member who has extensive knowledge of the project, including architecture
and design principles. Maintainers have the authority to merge PRs and make significant project decisions.

Responsibilities:

- All responsibilities of an Approver.
- Merge PRs to the main branch.
- Ensure the overall quality, stability, and performance of the codebase.
- Coordinate releases and update documentation.
- Guide the project's technical direction and roadmap.
- Facilitate communication and collaboration within the community.

Requirements:

- Demonstrated long-term, high-quality contributions and leadership in the project.
- Deep understanding of the project’s architecture and design.
- Commitment to the project's goals and values.
- Nominated and approved by existing Maintainers and Administrators.

### Maintainership

Maintainers of Dragonfly share the responsibility. And they have 3 things in general:

- share responsibility in the project's success;
- make a long-term investment to improve the project;
- spend time doing whatever needs to, not only the most interesting part;

Maintainers are often working hard, but sometimes what they do is hardly appreciate.
It is easy to work on the fancy part or technically advanced feature. It is harder
to work on minor bugfix, small improvement, long-term stable optimization or
others. While all of the above is the essential parts to build up a successful project.

### Adding Maintainers

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

### Removal of Inactive Maintainers

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

### How to make decision

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

### Updating Governance

All substantive updates in Governance require a supermajority maintainers vote.

## Administrator

An Administrator has the highest level of access and responsibility in the project. They manage overall project governance,
ensure adherence to policies, and resolve conflicts within the community.

Responsibilities:

- All responsibilities of a Maintainer.
- Manage project settings and permissions.
- Ensure compliance with legal and organizational policies.
- Handle CoC violations and mediate conflicts.
- Facilitate election of new roles or changes in the governance structure.

Requirements:

- Proven contribution history and leadership within the project.
- Strong understanding of open-source community management.
- Trusted by the community and other Administrators.
- Appointed through consensus of current Administrators.
