#!/usr/bin/env bash

echo "-------------------------------------------------------------------------------------------------------------"

while true ; do

#    for s in ascaris:1 hookworm:1 mansoni:4 whipworm:4 ; do
    for s in ascaris:1 ; do
        short=$(echo $s|cut -f 1 -d :)
        zone=$(echo $s|cut -f 2 -d :)
        printf '%-9s| ' "${short}"
        date | tr '\n' ' '
        echo -n '| '
        res=$(gcloud compute ssh ntd-endgame-model-runner-${short}-128 --project=artrabbit-clients-ntd --zone=europe-west${zone}-b --command='ps uxf|grep "results/"|grep -vE "(grep|tail)"' | awk '{print $14}' )
        iu=$( echo $res | awk -F '-' '{print $3}' | sed 's/\.out$//g' )
        line=$( ag $iu sch-sth-tra/iu-disease-data/mansoniIUs_scenario0.csv | cut -f 1 -d :| tr '\n' ' ' )
        echo -n "$iu | "
        printf '%-8s| ' "$line"
        echo "$res"
    done

    sleep 180
    continue

#    for z in north1 west2 west8 west9 ; do
    for z in north1 ; do
        for n in a b ; do
            host="${z}b-${n}"
            zone="${z}-b"
            printf '%-9s| ' "${host}"
            date | tr '\n' ' '
            echo -n '| '
            res=$(gcloud compute ssh ntd-endgame-model-runner-mansoni-${host}-128 --project=artrabbit-clients-ntd --zone=europe-${zone} --command='ps uxf|grep "results/"|grep -vE "(grep|tail)"' | awk '{print $14}')
            iu=$( echo $res | awk -F '-' '{print $3}' | sed 's/\.out$//g' )
            line=$( ag $iu sch-sth-tra/iu-disease-data/mansoniIUs_scenario0.csv | cut -f 1 -d : | tr '\n' ' ' )
            echo -n "$iu | "
            printf '%-8s| ' "$line"
            echo "$res"
        done
    done

    echo "-------------------------------------------------------------------------------------------------------------"
    sleep 180

done
