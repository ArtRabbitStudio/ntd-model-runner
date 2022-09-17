#!/usr/bin/env bash
while true ; do
    echo "------------------------------------------------------------------------------------------------"
    for s in ascaris hookworm ; do
        printf '%-9s| ' "${s}"
        date | tr '\n' ' '
        echo -n '| '
        gcloud compute ssh ntd-endgame-model-runner-$s-128 --project=artrabbit-clients-ntd --zone=europe-west1-b --command='ps uxf|grep "results/"|grep -vE "(grep|tail)"' | awk '{print $14}'
    done
    for s in mansoni whipworm ; do
        printf '%-9s| ' "${s}"
        date | tr '\n' ' '
        echo -n '| '
        gcloud compute ssh ntd-endgame-model-runner-$s-128 --project=artrabbit-clients-ntd --zone=europe-west4-b --command='ps uxf|grep "results/"|grep -vE "(grep|tail)"' | awk '{print $14}'
    done
    sleep 180
done
