#!/usr/bin/env bash

short=Man
num_sims=5
demogName=KenyaKDHS
uncompressed="-u"
local_storage="-l"
output_folder="sch-002"
result_folder=results/$( date +%Y%m%d%H%M%S )

function run_scenarios () {

    for scenario in 1 2 3a 3b ; do

        lines=$( grep -v Group_name iu-disease-data/mansoniIUs_zanzibar.csv )

        for line in $lines ; do

            iu=$( echo $line | cut -f 3 -d , )
            group=$( echo $line | cut -f 1 -d , | cut -f 1 -d _ )
            sub_scenario=$( echo $line | cut -f 4 -d , | tr -d '\r')
            full_scenario="${scenario}_${sub_scenario}"

            cmd="time python3 -u run.py -d $short -g $group -i $iu -s ${full_scenario} -n $num_sims -m $demogName -o $output_folder $uncompressed $local_storage"
            echo $cmd
            continue
            execute $group $iu $full_scenario "$cmd"

        done

    done

}

function execute () {

    local_group=$1
    local_iu=$2
    local_scenario=$3
    CMD=$4

    if [[ -n $local_storage ]] ; then
        maybe_fetch_files $local_iu
    fi

    output_file=$( printf "$result_folder/s${local_scenario}-g%03d-${local_iu}.out" ${local_group} )

    echo "*--> running '${CMD}' into ${output_file}"

    platform=$( uname -s )

    case $platform in

        Linux)
            unbuffer bash -c "${CMD}" | tee -a ${output_file}
            ;;

        Darwin)
            bash -c "${CMD}"
            ;;

        *)
            echo "==> don't know what to do on ${platform}"
            ;;
    esac

    echo
}

function maybe_fetch_files () {

    local_iu=$1

    local_p_file="data/input/Man_${local_iu}.p"

    if [[ ! -f ${local_p_file} ]] ; then
        remote_p_file="https://storage.googleapis.com/ntd-disease-simulator-data/diseases/sch-mansoni/source-data/${local_iu:0:3}/${local_iu}/Man_${local_iu}.p"
        echo "*--> fetching ${local_iu} .p file: $remote_p_file"
        curl -o ${local_p_file} ${remote_p_file}
    else
        echo "*--> already got $local_p_file locally"
    fi

    local_csv_file="data/input/Input_Rk_Man_${local_iu}.csv"

    if [[ ! -f ${local_csv_file} ]] ; then
        remote_csv_file="https://storage.googleapis.com/ntd-disease-simulator-data/diseases/sch-mansoni/source-data/${local_iu:0:3}/${local_iu}/Input_Rk_Man_${local_iu}.csv"
        echo "*--> fetching ${local_iu} .csv file: ${remote_csv_file}"
        curl -o ${local_csv_file} ${remote_csv_file}
    else
        echo "*--> already got $local_csv_file locally"
    fi
}

#mkdir -p $result_folder
run_scenarios
