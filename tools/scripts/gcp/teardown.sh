#!/usr/bin/env bash
# Deletes the cairn-* instances (labelled project=cairn), firewall rules, subnet and network, and
# nothing else. Shows the instances and asks unless --yes is given.
set -euo pipefail
cd "$(dirname "$0")"; source ./common.sh
$G compute instances list --filter="labels.project=cairn" --format="table(name,zone.basename(),status)"
if [ "${1:-}" != "--yes" ]; then read -r -p "Delete these instances and the cairn network? [y/N] " a; [ "$a" = y ]; fi
names=$($G compute instances list --filter="labels.project=cairn AND name~^cairn-" --format="value(name)")
[ -n "$names" ] && $G compute instances delete $names --zone "$ZONE" --delete-disks=all
for r in cairn-ssh cairn-internal; do $G compute firewall-rules delete "$r" 2>/dev/null || true; done
$G compute networks subnets delete "$SUBNET" --region "$REGION" 2>/dev/null || true
$G compute networks delete "$NET" 2>/dev/null || true
$G compute instances list --filter="labels.project=cairn"
