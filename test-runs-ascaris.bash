#!/usr/bin/env bash

# 100 then 50 sims
for d in ascaris:Asc mansoni:Man ; do
    species=$( echo $d | cut -f 1 -d : )
    short=$( echo $d | cut -f 2 -d : )
    for n in 100 50 ; do
            # 3 runs to get averages
            for s in 1 2 3 ; do
                    result_folder="results/$species/$n-$s"
                    mkdir -p $result_folder
                    for i in COD13959:4 COD14176:38 COD13830:21 ; do
                        iu=$( echo $i | cut -f 1 -d : )
                        group=$( echo $i | cut -f 2 -d : )
                        unbuffer bash -c "time python3 -u run.py -d $short -i $iu -n $n -g $group" | tee -a $result_folder/$iu.out
                    done
            done
    done
done
