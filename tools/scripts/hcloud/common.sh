# Shared settings for the Hetzner scale runs. Every resource carries the label project=cairn;
# teardown deletes by that label only, so other servers in the project are never touched.
LABEL="project=cairn"
LOCATION="${CAIRN_HC_LOCATION:-hel1}"
IMAGE="${CAIRN_HC_IMAGE:-ubuntu-24.04}"
NODE_TYPE="${CAIRN_HC_NODE_TYPE:-ccx43}"     # 16 dedicated vCPU, 64 GB, 360 GB NVMe
BENCH_TYPE="${CAIRN_HC_BENCH_TYPE:-ccx33}"   # 8 dedicated vCPU, 32 GB: client + ground truth
NODES="${CAIRN_HC_NODES:-3}"
NET_NAME="cairn-net"
NET_RANGE="10.77.0.0/16"
FW_NAME="cairn-fw"
KEY_NAME="cairn-scale"
KEY_FILE="$HOME/.ssh/cairn_hcloud"
SSH_OPTS="-i $KEY_FILE -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$HOME/.ssh/cairn_hcloud_known_hosts"
node_name() { echo "cairn-node$1"; }
node_ip() { echo "10.77.0.$((10 + $1))"; }   # private address of node i
BENCH_NAME="cairn-bench"
BENCH_IP="10.77.0.100"
public_ip() { hcloud server ip "$1"; }
