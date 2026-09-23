# Shared settings for the GCP scale runs. Every resource is named cairn-* and labelled
# project=cairn (instances); teardown deletes only those names.
PROJECT="${CAIRN_GCP_PROJECT:?set CAIRN_GCP_PROJECT to your GCP project id}"
REGION="${CAIRN_GCP_REGION:-europe-west1}"
ZONE="${CAIRN_GCP_ZONE:-europe-west1-b}"
NODE_TYPE="${CAIRN_GCP_NODE_TYPE:-n2-highmem-8}"   # 8 vCPU, 64 GB
BENCH_TYPE="${CAIRN_GCP_BENCH_TYPE:-e2-standard-4}" # 4 vCPU, 16 GB: client + ground truth
NODES="${CAIRN_GCP_NODES:-3}"
NET="cairn-net"; SUBNET="cairn-subnet"; RANGE="10.77.0.0/24"
KEY_FILE="$HOME/.ssh/cairn_hcloud"
SSH_USER="cairn"
SSH_OPTS="-i $KEY_FILE -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$HOME/.ssh/cairn_gcp_known_hosts -o ConnectTimeout=10"
G="gcloud --project $PROJECT --quiet"
node_name() { echo "cairn-node$1"; }
node_ip() { echo "10.77.0.$((10 + $1))"; }
BENCH_NAME="cairn-bench"; BENCH_IP="10.77.0.100"
public_ip() { $G compute instances describe "$1" --zone "$ZONE" --format='value(networkInterfaces[0].accessConfigs[0].natIP)'; }
