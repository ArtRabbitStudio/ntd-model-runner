#!/usr/bin/env bash

set -E
trap '[ "$?" -ne 128 ] || exit 128' ERR

NUM_INSTANCES=${NUM_INSTANCES:-10}
PROJECT="artrabbit-clients-ntd"
TIMESTAMP=$( date +%Y%m%d%H%M%S )

BACKUP_SET=$( gcloud compute images list --no-standard-images --project=artrabbit-clients-ntd | grep --color=none -E '.*-backup-[0-9]{14}' | awk '{print $1}' | cut -f 3 -d '-' | grep -E -v '\D'  | sort | uniq -c | awk '{print $2 ":(" $1 ")"}' )
echo "-> choose backup set to recreate instances from:"
BACKUP_STAMP=$( select stamp in ${BACKUP_SET} cancel ; do
    case ${stamp} in

        "cancel")
            exit 128
            break
            ;;

        [[:digit:]]*)
            echo ${stamp} | cut -f 1 -d ':'
            break
            ;;

        *)
            ;;

    esac
done )

echo "-> ok, about to recreate first ${NUM_INSTANCES} instances from backup timestamp ${BACKUP_STAMP}:"
gcloud compute images list --no-standard-images --project=artrabbit-clients-ntd | grep --color=none "${BACKUP_STAMP}" | sort -V | awk '{print $1}' | head -${NUM_INSTANCES}
echo "-> continue?"
select reply in yes no ; do
    case ${reply} in
        yes)
            break
            ;;
        *)
            echo "ok, exiting.";
            exit 0
            ;;
    esac
done

time parallel --colsep ':' -a <( for s in $( gcloud compute images list --project=artrabbit-clients-ntd --no-standard-images --format=json| jq -r '.[]|.name + "," + .sourceDisk'|grep --color=none -E '^temp[0-9]' | sort -V | grep "${BACKUP_STAMP}" ) ; do

    machine=$( echo $s | cut -f 1 -d '-' )
    backup=$( echo $s | cut -f 1 -d , )
    zone=$( echo $s | cut -f 9 -d / )

    echo $machine:$backup:$zone
done | head -${NUM_INSTANCES} ) "gcloud compute instances create {1} --scopes=storage-full,compute-ro --boot-disk-type=pd-ssd --machine-type=n2d-highcpu-128 --image=projects/artrabbit-clients-ntd/global/images/{2} --zone={3} --project=artrabbit-clients-ntd; echo"

