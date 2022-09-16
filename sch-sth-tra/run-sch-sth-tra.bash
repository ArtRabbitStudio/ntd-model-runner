#!/usr/bin/env bash

# default values
uncompressed=""
local_storage=""
demogName=KenyaKDHS
result_folder=results/$( date +%Y%m%d%H%M%S )
iu_list_file=""
scenarios="0,1,2,3a,3b"
source_data_path="source-data"

# map of e.g. Tra -> 'trachoma' for source data URL paths
declare -A DISEASE_SHORT_NAMES_TO_CODES
DISEASE_SHORT_NAMES_TO_CODES[Tra]="trachoma"
DISEASE_SHORT_NAMES_TO_CODES[Tri]="sth-whipworm"
DISEASE_SHORT_NAMES_TO_CODES[Asc]="sth-roundworm"
DISEASE_SHORT_NAMES_TO_CODES[Hook]="sth-hookworm"
DISEASE_SHORT_NAMES_TO_CODES[Man]="sch-mansoni"

function usage() {
    echo "usage: ${0}"
    echo "            -d <short-disease> -s <scenario-list> -i <iu-list-file> -n <num-sims> -o <output-folder>"
    echo "            [-p <source-data-path>] [-u (uncompressed)] [-l (local_storage)]"
    echo "            [-r <read-pickle-file-suffix>] [-f <save-pickle-file-suffix>] [-b <burn-in-years>]"
    exit 1
}

function run_scenarios () {

    for scenario in ${scenarios//,/ } ; do

        lines=$( grep -v IU_ID ${iu_list_file} | sed 's/"//g' )

        for line in $lines ; do

            case "${disease}" in

                Hook|Tri|Asc|Tra)
                    iu=$( echo $line | cut -f 3 -d , )
                    group=$( echo $line | cut -f 1 -d , )
                    cmd_options="-g ${group} -i ${iu} -s ${scenario}"
                    full_scenario="${scenario}"
                    ;;

                Man)
                    iu=$( echo $line | cut -f 3 -d , )
                    group=$( echo $line | cut -f 1 -d , | cut -f 1 -d _ )
                    sub_scenario=$( echo $line | cut -f 4 -d , | tr -d '\r')
                    full_scenario="${scenario}_${sub_scenario}"
                    cmd_options="-g ${group} -i ${iu} -s ${full_scenario}"
                    ;;
                
                *)
                    usage
                    ;;

            esac

            if [[ -z "${read_pickle_file_suffix}" ]] ; then
                read_pickle_cmd=""
            else
                read_pickle_cmd="-r ${read_pickle_file_suffix}"
            fi

            if [[ -z "${save_pickle_file_suffix}" ]] ; then
                save_pickle_cmd=""
            else
                save_pickle_cmd="-f ${save_pickle_file_suffix}"
            fi

            if [[ -z "${burn_in_time}" ]] ; then
                burn_in_time_cmd=""
            else
                burn_in_time_cmd="-b ${burn_in_time}"
            fi

            cmd="time python3 -u run.py -d ${disease} ${cmd_options} -n ${num_sims} -m ${demogName} -o ${output_folder} -p ${source_data_path} ${read_pickle_cmd} ${save_pickle_cmd} ${burn_in_time_cmd} ${uncompressed} ${local_storage}"

            if [[ "${DISPLAY_CMD:=n}" == "y" ]] ; then
                echo $cmd
                continue
            else
                execute $group $iu $full_scenario "$cmd"
            fi

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

    if [[ "${disease}" = "Tra" ]] ; then
        p_file_name="OutputVals_${local_iu}.p"
    else
        p_file_name="${disease}_${local_iu}.p"
    fi

    local_p_file="data/input/${p_file_name}"

    if [[ ! -f ${local_p_file} ]] ; then
        remote_p_file="https://storage.googleapis.com/ntd-disease-simulator-data/diseases/${DISEASE_SHORT_NAMES_TO_CODES[${disease}]}/${source_data_path}/${local_iu:0:3}/${local_iu}/${p_file_name}"
        echo "*--> fetching ${local_iu} .p file: $remote_p_file"
        curl -o ${local_p_file} ${remote_p_file}
    else
        echo "*--> already got $local_p_file locally"
    fi

    if [[ "${disease}" = "Tra" ]] ; then
        csv_file_name="InputBet_${local_iu}.csv"
    else
        csv_file_name="Input_Rk_${disease}_${local_iu}.csv"
    fi

    local_csv_file="data/input/${csv_file_name}"

    if [[ ! -f ${local_csv_file} ]] ; then
        remote_csv_file="https://storage.googleapis.com/ntd-disease-simulator-data/diseases/${DISEASE_SHORT_NAMES_TO_CODES[${disease}]}/${source_data_path}/${local_iu:0:3}/${local_iu}/${csv_file_name}"
        echo "*--> fetching ${local_iu} .csv file: ${remote_csv_file}"
        curl -o ${local_csv_file} ${remote_csv_file}
    else
        echo "*--> already got $local_csv_file locally"
    fi
}

# work out disease, num sims, output folder, file options
while getopts ":d:s:i:n:o:p:r:f:b:ulh" opts ; do

    case "${opts}" in

        d)
            disease=${OPTARG}
            ;;

        s)
            scenarios=${OPTARG}
            ;;

        i)
            iu_list_file=${OPTARG}
            ;;

        n)
            num_sims=${OPTARG}
            ;;

        o)
            output_folder=${OPTARG}
            ;;

        p)
            source_data_path=${OPTARG}
            ;;

        r)
            read_pickle_file_suffix=${OPTARG}
            if [[ "${read_pickle_file_suffix:0:1}" == "-" ]] ; then
                usage
            fi
            ;;

        f)
            save_pickle_file_suffix=${OPTARG}
            if [[ "${save_pickle_file_suffix:0:1}" == "-" ]] ; then
                usage
            fi
            ;;

        b)
            burn_in_time=${OPTARG}
            if [[ "${burn_in_time:0:1}" == "-" ]] ; then
                usage
            fi
            ;;

        u)
            uncompressed="-u"
            ;;

        l)
            local_storage="-l"
            ;;

        h)
            usage
            ;;

        *)
            usage
            ;;

    esac

done

if [[ -z "${disease}" || -z "${num_sims}" || -z "${output_folder}" || ! -f "${iu_list_file}" ]] ; then
    usage
fi

if [[ "${DISPLAY_CMD:=n}" != "y" ]] ; then
    mkdir -p $result_folder
fi

if [[ "${save_pickle_file_suffix}" = "${read_pickle_file_suffix}" ]] ; then
    echo "error: output pickle file suffix must be different from input pickle file suffix" >&2
    usage
fi

if [[ -n "${save_pickle_file_suffix}" && -z "${burn_in_time}" ]] ; then
    echo "error: burn-in requires a number of years" >&2
    usage
fi

run_scenarios
