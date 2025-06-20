#!/usr/bin/env bash

PROJECT="artrabbit-clients-ntd"
TIMESTAMP=$( date +%Y%m%d%H%M%S )

time parallel \
    --colsep ':' \
    -j $(( $( sysctl -n hw.physicalcpu ) -1 )) \
    -a <( gcloud compute instances list --project=${PROJECT} | grep --color=none -E 'temp[0-9][0-9]? ' | sort -V | awk '{print $1":"$2}' ) \
    gcloud compute images create "{1}-backup-${TIMESTAMP}"  --source-disk={1} --source-disk-zone={2} --project=artrabbit-clients-ntd 
