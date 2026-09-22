#!/bin/sh
# udpshunt post-remove: stop and disable the service, but only on a real
# removal. Upgrades also run this scriptlet (rpm "$1"=2, deb "upgrade") and
# must not touch the service there. The config file is a conffile and stays
# on disk for a later reinstall.
set -e

case "$1" in
    0 | remove | purge) ;;
    *) exit 0 ;; # upgrade or unknown: leave the service alone
esac

if ! command -v systemctl >/dev/null 2>&1; then
    exit 0
fi

systemctl stop udpshunt >/dev/null 2>&1 || true
systemctl disable udpshunt >/dev/null 2>&1 || true
systemctl daemon-reload >/dev/null 2>&1 || true
echo "udpshunt removed (config /etc/udpshunt/udpshunt.yaml kept)."
