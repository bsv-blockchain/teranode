#!/bin/bash

# print the environment variables for <MGS_IP> and <FS_NAME>
echo "Mounting Lustre filesystem..."
echo "MGS_IP: $MGS_IP"
echo "FS_NAME: $FS_NAME"

# Mount the Lustre filesystem
mkdir -p /data
mount -t lustre $MGS_IP@tcp:/$FS_NAME /data

# Remove files older than EXPIRY_MINS
echo "Removing files older than $EXPIRY_MINS days..."

# find /data/ -maxdepth 1 -type f -mmin +$EXPIRY_MINS -delete
find t1/subtreestore/s3 -maxdepth 1 -type f -mmin +$EXPIRY_MINS  -delete
find t2/subtreestore/s3 -maxdepth 1 -type f -mmin +$EXPIRY_MINS  -delete
find t3/subtreestore/s3 -maxdepth 1 -type f -mmin +$EXPIRY_MINS  -delete
find t1/blockstore/s3 -maxdepth 1 -type f -mmin +$EXPIRY_MINS  -delete
find t2/blockstore/s3 -maxdepth 1 -type f -mmin +$EXPIRY_MINS  -delete
find t3/blockstore/s3 -maxdepth 1 -type f -mmin +$EXPIRY_MINS  -delete

# Unmount the Lustre filesystem
echo "Unmounting Lustre filesystem..."
umount /data

echo "Lustre cleanup job completed."