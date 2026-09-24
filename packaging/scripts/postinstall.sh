#!/bin/sh
# udpshunt post-install: register and (re)start the systemd service, and on
# start failure print the service's own error output right into the install
# log ("why did it not start" answered without a second command).
#
# Shared by rpm, deb and apk. rpm passes "1" (install) / "2" (upgrade) in $1;
# deb passes "configure <new>" on install and "configure <old> <new>" on
# upgrade; apk and anything else falls through to the install path.
#
# Every systemctl call is non-fatal: a scriptlet failure would roll back the
# whole package transaction, so a broken or foreign init system must never
# block installing the files.
set -e

if ! command -v systemctl >/dev/null 2>&1; then
    echo "udpshunt: systemctl not found (OpenRC or other init); skipping service setup."
    exit 0
fi

systemctl daemon-reload >/dev/null 2>&1 || true

# fix_blacklist_ownership: the blacklist API persists entries by creating a
# temp file next to the configured list file, so that directory must be
# writable by the service user (User=nobody in the shipped unit). Packages
# install /etc/udpshunt root-owned, which would make every API mutation
# fail with "permission denied" (observed on CentOS 7: open .../blacklist-*.tmp:
# permission denied). chown the directory — and the list file when it exists —
# to the service user. User-only chown: the nobody group is named differently
# across distros (nobody/nogroup). Non-fatal by design; a failure here only
# means API persistence needs the manual fix described in the README.
fix_blacklist_ownership() {
    cfg=/etc/udpshunt/udpshunt.yaml
    [ -f "$cfg" ] || return 0
    bf=$(awk '
        /^blacklist:/       { inblk = 1; next }
        inblk && /^[^ \t#]/ { inblk = 0 }
        inblk && /^[ \t]*file:/ {
            sub(/^[ \t]*file:[ \t]*/, "")
            gsub(/"/, "")
            print
            exit
        }' "$cfg") || true
    [ -n "$bf" ] || return 0
    case "$bf" in
        /*) ;;
        *) echo "udpshunt: blacklist file '$bf' is not an absolute path; chown it by hand (see README)."
           return 0 ;;
    esac
    chown nobody "$bf" 2>/dev/null || true
    bdir=$(dirname "$bf")
    if chown nobody "$bdir" 2>/dev/null; then
        echo "udpshunt: blacklist dir $bdir chowned to the service user (API persistence)."
    else
        echo "udpshunt: WARNING: could not chown $bdir; blacklist API writes may fail (see README)."
    fi
}
fix_blacklist_ownership

# report_if_dead prints the service's exit status and its last journal lines
# when it is not running, so a failed start is visible in the install output.
report_if_dead() {
    if ! systemctl is-active --quiet udpshunt; then
        echo "udpshunt: service is NOT running. Reason:"
        systemctl status udpshunt --no-pager --lines=0 2>&1 | sed -n '1,6p' || true
        journalctl -u udpshunt -n 10 --no-pager 2>&1 | tail -n 10 || true
        echo "udpshunt: fix the config (/etc/udpshunt/udpshunt.yaml), then: systemctl restart udpshunt"
    fi
}

fresh=1
case "$1" in
    2) fresh=0 ;;                       # rpm upgrade
    configure) [ -n "$3" ] && fresh=0 ;; # deb: $3 set only on upgrade
esac

if [ "$fresh" = 1 ]; then
    systemctl enable udpshunt >/dev/null 2>&1 || true
    echo "udpshunt installed and enabled (starter config: loopback demo listener)."
    echo "  config: /etc/udpshunt/udpshunt.yaml"
    echo "  start:  systemctl start udpshunt   (systemctl status udpshunt shows the reason if it fails)"
else
    was_active=0
    systemctl is-active --quiet udpshunt && was_active=1
    systemctl try-restart udpshunt >/dev/null 2>&1 || true
    echo "udpshunt upgraded; service restarted (it was running)."
    # A service the admin had stopped stays stopped — only diagnose when the
    # restart of a previously running service left it dead.
    if [ "$was_active" = 1 ]; then
        report_if_dead
    fi
fi
