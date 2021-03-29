%undefine _disable_source_fetch

Name:    dfget
Version: %{_dfget_version}
Release: alpha.1
Summary: Dragonfly dfget
URL:     https://d7y.io/
License: Apache 2.0
Source0: dfget
Source1: dfget-daemon.service
Source2: dfget-daemon.yaml
Source3: dfget.1
Source4: LICENSE
Source5: ChangeLog
BuildRoot: %{_topdir}/BUILD/%{name}-%{version}-%{release}
BuildArch: x86_64
Requires(pre): shadow-utils

%{?systemd_requires}
BuildRequires: systemd

%description
%{summary}

%install
mkdir -p %{buildroot}%{_bindir}
mkdir -p %{buildroot}%{_sysconfdir}/dragonfly
install -p -m 755 %{SOURCE0} %{buildroot}%{_bindir}/dfget
install -D -m 644 %{SOURCE1} %{buildroot}%{_unitdir}/dfget-daemon.service
install -D -m 600 %{SOURCE2} %{buildroot}%{_sysconfdir}/dragonfly/dfget-daemon.yaml
install -D -m 644 %{SOURCE3} %{buildroot}%{_mandir}/man1/dfget.1
install -D -m 644 %{SOURCE4} %{buildroot}%{_docdir}/dfget/LICENSE
install -D -m 644 %{SOURCE5} %{buildroot}%{_docdir}/dfget/ChangeLog

%files
%defattr(755,root,root)
%{_bindir}/dfget
%{_docdir}/dfget
%defattr(644,root,root)
%{_unitdir}/dfget-daemon.service
%{_mandir}/man1/dfget.1*
%{_docdir}/dfget/*
%defattr(600,root,root)
%config(noreplace) %{_sysconfdir}/dragonfly/dfget-daemon.yaml

%pre
#getent group dragonfly >/dev/null || groupadd -r dragonfly
#getent passwd dragonfly >/dev/null || \
#    useradd -r -g dragonfly -d /dev/null -s /sbin/nologin \
#    -c "dfget Daemon" dragonfly
exit 0

%post
%systemd_post dfget-daemon.service

%preun
%systemd_preun dfget-daemon.service

%postun
%systemd_postun_with_restart dfget-daemon.service

%changelog
