#!/usr/bin/env bash
# Deletes every resource labelled project=cairn (servers, network, firewall, SSH key), and only
# those. Shows the list and asks for confirmation unless --yes is given.
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
hcloud server list -l "$LABEL"
if [ "${1:-}" != "--yes" ]; then read -r -p "Delete the servers above and the cairn network/firewall/key? [y/N] " a; [ "$a" = y ]; fi
for s in $(hcloud server list -l "$LABEL" -o noheader -o columns=name); do hcloud server delete "$s"; done
for n in $(hcloud network list -l "$LABEL" -o noheader -o columns=name); do hcloud network delete "$n"; done
for f in $(hcloud firewall list -l "$LABEL" -o noheader -o columns=name); do hcloud firewall delete "$f"; done
for k in $(hcloud ssh-key list -l "$LABEL" -o noheader -o columns=name); do hcloud ssh-key delete "$k"; done
hcloud server list -l "$LABEL"
