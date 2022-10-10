#!/usr/bin/env bash

set -euo pipefail

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
    echo "            -d <short-disease-code> -s <scenario-list> -i <iu-list-file> -n <num-sims> -o <output-folder>"
    echo "            [-p <source-data-path>] [-u (uncompressed)] [-l (local_storage)]"
    echo "            [-r <read-pickle-file-suffix>] [-f <save-pickle-file-suffix>] [-b <burn-in-years>]"
    echo "            [-g <segment-number,out-of> ]"
    exit 1
}

function run_scenarios () {

    for scenario in ${scenarios//,/ } ; do

        lines=$( get_lines ${segment_number:=} ${out_of:=} ${iu_list_file} )

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

            if [[ -z "${burn_in_time:=}" ]] ; then
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

function get_lines () {

    # if there's no segment_number/out_of variables then just strip & output all the lines from the file
    if [[ -z ${1:-} || -z ${2:-} ]] ; then
        grep -v IU_ID ${iu_list_file} | sed 's/"//g'
        return
    fi

    # otherwise work out the appropriate segment of lines to use
    local seg=$1
    local requested_segments=$2
    local file=$3

    local line_count=$(( $( wc -l < ${file} | awk '{print $1}' ) - 1 ))

    local num_per_segment=$(( $line_count / $requested_segments ))
    local remainder=$(( $line_count % ( num_per_segment * $requested_segments )  ))
    local total_from_segments=$(( $requested_segments * $num_per_segment ))
    echo "-> $requested_segments segments * $num_per_segment lines makes $total_from_segments lines total, leaving a remainder of ${remainder} from a ${line_count}-line file, using a last segment of $(( num_per_segment + remainder )) to make a total of $(( $total_from_segments + $remainder )) " >&2

    local first_line=$(( $num_per_segment * ( $seg - 1 ) + 1 ))
    local last_line=$(( ( $first_line + $num_per_segment ) - 1 ))

    if [[ ${seg} = ${requested_segments} ]] ; then
        last_line=$(( $last_line + $remainder ))
    fi
    local end_line=$(( $last_line + 1 ))

    local segment_line_count=$(( ( last_line - first_line ) + 1 ))
    echo "-> segment number ${seg} has ${segment_line_count} lines, from $first_line - $last_line inclusive" >&2

    grep -v IU_ID ${iu_list_file} | sed 's/"//g' | sed -n "${first_line},${last_line}p;${end_line}q"

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
while getopts ":d:s:i:n:o:p:r:f:g:b:ulh" opts ; do

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

        g)
            segment_number_out_of=${OPTARG}
            if [[ -z "$( echo ${segment_number_out_of} | grep , )" ]] ; then
                usage
            fi

            segment_number=$( echo ${segment_number_out_of} | cut -f 1 -d , )
            out_of=$( echo ${segment_number_out_of} | cut -f 2 -d , )

            if [[ ${segment_number} -le 0 || ${out_of} -le 0 ]] ; then
                echo "error: segment number/total number of segments are invalid" >&2
                usage
            fi

            if [[ ${segment_number} -gt ${out_of} ]] ; then
                echo "error: segment number ${segment_number} is larger than total number of segments ${out_of}" >&2
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

if [[ -z "${disease:=}" || -z "${num_sims:=}" || -z "${output_folder:=}" || ! -f "${iu_list_file:=}" ]] ; then
    usage
fi

if [[ "${DISPLAY_CMD:=n}" == "y" ]] ; then
    echo "making folder"
    #mkdir -p $result_folder
fi

if [[ "${save_pickle_file_suffix:=}" = "${read_pickle_file_suffix:=}" ]] && [[ -n "${read_pickle_file_suffix:=}" ]]; then
    echo "error: output pickle file suffix must be different from input pickle file suffix" >&2
    usage
fi

if [[ -n "${save_pickle_file_suffix:=}" && -z "${burn_in_time:=}" ]] ; then
    echo "error: burn-in requires a number of years" >&2
    usage
fi

run_scenarios
