#!/bin/bash
# Upgrade edificia-server disk from pd-standard (HDD) to pd-balanced (SSD)
# Requires ~5 min downtime. Cost: +$1.80/month ($1.20 → $3.00 for 30GB)
#
# Performance improvement:
#   IOPS: 22.5 → 180 (8x)
#   Throughput: 3.6 MB/s → 8.4 MB/s (2.3x)
#
# Run from local machine with gcloud auth.

set -e

ZONE="us-west1-a"
INSTANCE="edificia-server"
DISK="edificia-server"
SNAPSHOT="edificia-disk-snapshot-$(date +%Y%m%d)"
NEW_DISK="edificia-server-ssd"

echo "Step 1: Stop instance"
gcloud compute instances stop $INSTANCE --zone=$ZONE

echo "Step 2: Create snapshot of current disk"
gcloud compute disks snapshot $DISK --zone=$ZONE --snapshot-names=$SNAPSHOT

echo "Step 3: Create new pd-balanced disk from snapshot"
gcloud compute disks create $NEW_DISK \
  --zone=$ZONE \
  --source-snapshot=$SNAPSHOT \
  --type=pd-balanced \
  --size=30GB

echo "Step 4: Detach old disk"
gcloud compute instances detach-disk $INSTANCE --zone=$ZONE --disk=$DISK

echo "Step 5: Attach new SSD disk"
gcloud compute instances attach-disk $INSTANCE --zone=$ZONE --disk=$NEW_DISK --boot

echo "Step 6: Start instance"
gcloud compute instances start $INSTANCE --zone=$ZONE

echo "Step 7: Wait for boot and verify"
sleep 30
gcloud compute ssh $INSTANCE --zone=$ZONE --command="python3 /home/juanwisznia/edificia/monitor.py"

echo ""
echo "Done. Old disk '$DISK' can be deleted after confirming everything works:"
echo "  gcloud compute disks delete $DISK --zone=$ZONE"
echo "  gcloud compute snapshots delete $SNAPSHOT"
