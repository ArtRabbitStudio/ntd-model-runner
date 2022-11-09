#!/usr/bin/env bash
# shellcheck disable=SC2143

# fail fast and hard
set -euo pipefail

# default values
demogName=KenyaKDHS
result_folder=results/$( date +%Y%m%d%H%M%S )
scenarios="0,1,2,3a,3b"
num_sims=0
source_data_path="source-data"
source_bucket="ntd-disease-simulator-data"
destination_bucket="ntd-endgame-result-data"

# empty default values
run_name=""
person_email=""
disease=""
iu_list_file=""
local_storage=""
output_folder=""
read_pickle_file_suffix=""
save_pickle_file_suffix=""
out_of=""
burn_in_time=""
uncompressed=""
local_storage=""

# map of e.g. Tra -> 'trachoma' for source data URL paths
declare -A DISEASE_SHORT_NAMES_TO_CODES
DISEASE_SHORT_NAMES_TO_CODES["Tra"]="trachoma"
DISEASE_SHORT_NAMES_TO_CODES["Tri"]="sth-whipworm"
DISEASE_SHORT_NAMES_TO_CODES["Asc"]="sth-roundworm"
DISEASE_SHORT_NAMES_TO_CODES["Hook"]="sth-hookworm"
DISEASE_SHORT_NAMES_TO_CODES["Man"]="sch-mansoni"

function usage() {
    echo "usage: ${0}"
    echo "    -d <short-disease-code> -i <iu-list-file> -n <num-sims> -N <run-name> -e <person-email>"
    echo "    [-s <scenario-list>]"
    echo "    [-p <source-data-path>] [-o <output-folder>]"
    echo "    [-k <source-bucket>] [-K destination-bucket>]"
    echo "    [-u (uncompressed)]"
    echo "    [-l (local_storage)]"
    echo "    [-r <read-pickle-file-suffix>]"
    echo "    [-f <save-pickle-file-suffix>] [-b <burn-in-years>]"
    echo "    [-g <iu-file-segment-number,out-of-total-segments>]"
    exit 1
}

# WARNING: these are all written to global variables
function get_options () {

    # work out disease, num sims, output folder, file options
    case "${1}" in

        d)
            disease=${OPTARG}
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

        N)
            run_name=$( echo -n ${OPTARG} | base64 )
            ;;

        e)
            person_email=${OPTARG}
            ;;

        s)
            scenarios=${OPTARG}
            ;;

        k)
            source_bucket=${OPTARG}
            ;;

        K)
            destination_bucket=${OPTARG}
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
            if [[ -z "$( echo "${segment_number_out_of}" | grep , )" ]] ; then
                usage
            fi

            segment_number=$( echo "${segment_number_out_of}" | cut -f 1 -d , )
            out_of=$( echo "${segment_number_out_of}" | cut -f 2 -d , )

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

}

function check_options () {

    # require basics - output_folder not needed as defaulting to a 'slugified' desc in run.py
    if [[ -z "${disease:=}" || -z "${num_sims:=}" || ! -f "${iu_list_file:=}" || -z "${run_name:=}" || -z "${person_email:=}" ]] ; then
        echo "error: disease, num_sims, iu_list_file, run_name and person_email are required" >&2
        usage
    fi

    # check not trying to read and write from/to same pickle fix
    if [[ "${save_pickle_file_suffix:=}" = "${read_pickle_file_suffix:=}" ]] && [[ -n "${read_pickle_file_suffix:=}" ]]; then
        echo "error: output pickle file suffix must be different from input pickle file suffix" >&2
        usage
    fi

    # require burn-in time for saving pickle files
    if [[ -n "${save_pickle_file_suffix:=}" && -z "${burn_in_time:=}" ]] ; then
        echo "error: burn-in requires a number of years" >&2
        usage
    fi

    # make sure we really want to add results to this run
    if [[ "${DISPLAY_CMD:=n}" != "y" ]] ; then

        # call local find_run.py script to get existing run info
        local existing_run_info=$( python find_run.py "${disease}" "${run_name}" 2>/dev/null )

        if [[ -n "${existing_run_info}" ]] ; then

            existing_run_id=$( echo $existing_run_info | cut -f 1 -d = )
            existing_run_count=$( echo $existing_run_info | cut -f 2 -d = )
            existing_run_start=$( echo $existing_run_info | cut -f 3 -d = )

            local plain_name=$( echo -n $run_name | base64 -d )
            # TODO get the previously-used output_folder for this run and re-use it, overriding any option provided here
            echo "---> confirm: do you really want to add results to the run named ${plain_name} (id ${existing_run_id}, started at ${existing_run_start} with ${existing_run_count} results) ?"
            select confirm in yes no ; do
                case "${confirm}" in
                    yes)
                        break
                        ;;
                    no)
                        echo "ok, exiting."
                        exit 0
                        ;;
                    *)
                        echo "eh?"
                        ;;
                esac
            done
        fi
    fi

    # create result folder if this is a real run
    if [[ "${DISPLAY_CMD:=n}" == "y" ]] ; then
        echo "(would be making folder $result_folder in a real run)" >&2
    else
        mkdir -p "$result_folder"
        if [[ ! -d "${result_folder}" ]] ; then
            echo "couldn't make/find result folder ${result_folder}"
           exit 1
        fi
    fi

}

function run_scenarios () {

    for scenario in ${scenarios//,/ } ; do

        lines=$( get_lines "${segment_number:=}" "${out_of:=}" "${iu_list_file}" )

        for line in $lines ; do

            case "${disease}" in

                Hook|Tri|Asc|Tra)
                    iu=$( echo "$line" | cut -f 3 -d , )
                    group=$( echo "$line" | cut -f 1 -d , )
                    cmd_options="-g ${group} -i ${iu} -s ${scenario}"
                    full_scenario="${scenario}"
                    ;;

                Man)
                    iu=$( echo "$line" | cut -f 3 -d , )
                    group=$( echo "$line" | cut -f 1 -d , | cut -f 1 -d _ )
                    sub_scenario=$( echo "$line" | cut -f 4 -d , | tr -d '\r')
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

            if [[ -z "${output_folder:=}" ]] ; then
                output_folder_cmd=""
            else
                output_folder_cmd="-o ${output_folder}"
            fi

            cmd="time python3 -u run.py -d ${disease} ${cmd_options} -n ${num_sims} -N ${run_name} -e ${person_email} -m ${demogName} -k ${source_bucket} -K ${destination_bucket} -p ${source_data_path} ${output_folder_cmd} ${read_pickle_cmd} ${save_pickle_cmd} ${burn_in_time_cmd} ${uncompressed} ${local_storage}"

            if [[ "${DISPLAY_CMD:=n}" == "y" ]] ; then
                echo "$cmd"
                continue
            else
                execute "$group" "$iu" "$full_scenario" "$cmd"
            fi

        done

    done

}

function get_lines () {

    # if there's no segment_number/out_of variables then just strip & output all the lines from the file
    if [[ -z ${1:-} || -z ${2:-} ]] ; then
        set +e
        grep -v IU_ID ${iu_list_file} | sed 's/"//g'
        set -e
        return
    fi

    # otherwise work out the appropriate segment of lines to use
    local seg=$1
    local requested_segments=$2
    local file=$3

    local line_count=$(( $( wc -l < "${file}" | awk '{print $1}' ) - 1 ))

    local num_per_segment=$(( line_count / requested_segments ))
    local remainder=$(( line_count % ( num_per_segment * requested_segments )  ))
    local total_from_segments=$(( requested_segments * num_per_segment ))
    echo \
        "-> $requested_segments segments * $num_per_segment lines makes $total_from_segments lines total," \
        "leaving a remainder of ${remainder} from a ${line_count}-line file," \
        "using a last segment of $(( num_per_segment + remainder ))" \
        "to make a total of $(( total_from_segments + remainder ))" >&2

    local first_line=$(( num_per_segment * ( seg - 1 ) + 1 ))
    local last_line=$(( ( first_line + num_per_segment ) - 1 ))

    if [[ ${seg} = "${requested_segments}" ]] ; then
        last_line=$(( last_line + remainder ))
    fi
    local end_line=$(( last_line + 1 ))

    local segment_line_count=$(( ( last_line - first_line ) + 1 ))
    echo "-> segment number ${seg} has ${segment_line_count} lines, from $first_line - $last_line inclusive" >&2

    # strip the lines from the file and only return the selected subset
    set +e
    grep -v IU_ID ${iu_list_file} | sed 's/"//g' | sed -n "${first_line},${last_line}p;${end_line}q"
    set -e

}

function execute () {

    local local_group=$1
    local local_iu=$2
    local local_scenario=$3
    CMD=$4

    if [[ -n $local_storage ]] ; then
        maybe_fetch_files "$local_iu"
    fi

    output_file=$( printf "$result_folder/s${local_scenario}-g%03d-${local_iu}.out" "${local_group}" )

    echo "*--> running '${CMD}' into ${output_file}"

    platform=$( uname -s )

    case $platform in

        Linux)
            unbuffer bash -c "${CMD}" | tee -a "${output_file}"
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

    local local_iu=$1

    if [[ "${disease}" = "Tra" ]] ; then
        p_file_name="OutputVals_${local_iu}.p"
    else
        p_file_name="${disease}_${local_iu}.p"
    fi

    local local_p_file="data/input/${p_file_name}"

    if [[ ! -f ${local_p_file} ]] ; then
        remote_p_file="https://storage.googleapis.com/${source_bucket}/diseases/${DISEASE_SHORT_NAMES_TO_CODES[${disease}]}/${source_data_path}/${local_iu:0:3}/${local_iu}/${p_file_name}"
        echo "*--> fetching ${local_iu} .p file: $remote_p_file"
        curl -o "${local_p_file}" "${remote_p_file}"
    else
        echo "*--> already got $local_p_file locally"
    fi

    if [[ "${disease}" = "Tra" ]] ; then
        csv_file_name="InputBet_${local_iu}.csv"
    else
        csv_file_name="Input_Rk_${disease}_${local_iu}.csv"
    fi

    local local_csv_file="data/input/${csv_file_name}"

    if [[ ! -f ${local_csv_file} ]] ; then
        remote_csv_file="https://storage.googleapis.com/${source_bucket}/diseases/${DISEASE_SHORT_NAMES_TO_CODES[${disease}]}/${source_data_path}/${local_iu:0:3}/${local_iu}/${csv_file_name}"
        echo "*--> fetching ${local_iu} .csv file: ${remote_csv_file}"
        curl -o "${local_csv_file}" "${remote_csv_file}"
    else
        echo "*--> already got $local_csv_file locally"
    fi
}

# call getopts in global scope to get argv for $0
while getopts ":d:s:i:n:N:e:o:k:K:p:r:f:g:b:ulh" opts ; do
    # shellcheck disable=SC2086
    get_options $opts
done

check_options
run_scenarios
