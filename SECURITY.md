# Security Policy

## Reporting a Vulnerability

To report a Dragonfly vulnerability:

1. Navigate to the security tab on the repository GitHub Security Tab.
2. Click on Advisories GitHub Advisories tab.
3. Click on Report a vulnerability.

The reporter(s) can expect a response within 24 hours acknowledging the issue was received.
If a response is not received within 24 hours, please reach out to any committer directly to confirm receipt of the issue.

## Review Process

Once a committer has confirmed the relevance of the report, a draft security advisory will be created on GitHub.
The draft advisory will be used to discuss the issue with committers, the reporter(s), and Dragonfly's security advisors.
If the reporter(s) wishes to participate in this discussion, then provide reporter GitHub username(s) to be invited
to the discussion. If the reporter(s) does not wish to participate directly in the discussion,
then the reporter(s) can request to be updated regularly via email.

If the vulnerability is accepted, a timeline for developing a patch, public disclosure, and patch release will be
determined. If there is an embargo period on public disclosure before the patch release,
an announcement will be sent to the security announce mailing list <dragonfly-developers@googlegroups.com> announcing
the scope of the vulnerability, the date of availability of the patch release, and the date of public disclosure.
The reporter(s) are expected to participate in the discussion of the timeline and abide by agreed-upon dates for public disclosure.

## Supported Versions

See the Dragonfly releases page for information on supported versions of Dragonfly.
Any Extended or Active release branch may receive security updates. For any security issues discovered
on older versions, non-core packages, or dependencies, please inform committers using
the same security mailing list as for reporting vulnerabilities.

## Joining the Security Announce Mailing List

Any organization or individual who directly uses Dragonfly and non-core packages in production or in a
security-critical application is eligible to join the security announce mailing list. Indirect users
who use Dragonfly through a vendor are not expected to join but should request their vendor join.
To join the mailing list, the individual or organization must be sponsored by either a Dragonfly committer or
security advisor as well as have a record of properly handling non-public security information.
Sponsorship should not be requested via public channels since membership of the security announce list is not public.

### Mailing Lists

- **dragonfly-developers@googlegroups.com**: Report security concerns.

### Confidentiality, Integrity, and Availability

We prioritize vulnerabilities that compromise data confidentiality, privilege elevation, or integrity.
Availability issues, such as DoS and resource exhaustion, are also serious concerns.
Operators must configure settings, role-based access control, and other features to provide a hardened environment.
