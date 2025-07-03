#!/usr/bin/env bash

# set up APT
echo "-> setting up apt sources list"
export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo tee /usr/share/keyrings/cloud.google.asc
echo "-> getting apt update"
sudo apt-get update -y --allow-releaseinfo-change 

# get gcsfuse
echo "-> installing gcsfuse"
sudo apt-get install -y gcsfuse

# create folder & setup permissions
echo "-> checking mount folder"
MOUNT_FOLDER=/mnt/gcs/ntd-disease-simulator-data
if [[ ! -d "${MOUNT_FOLDER}" ]] ; then
    sudo mkdir -p /mnt/gcs/ntd-disease-simulator-data
    sudo chmod -R 755 /mnt
fi
