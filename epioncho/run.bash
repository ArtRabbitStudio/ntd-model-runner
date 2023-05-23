#!/usr/bin/env bash

# bail out immediately if anything errors
set -euo pipefail

function info () {
	# shellcheck disable=SC2086,SC2027
    echo -e '\E[37;44m'"\033[1m-> "$1"\033[0m" 1>&2
    tput -T linux sgr0
}

function error () {
	# shellcheck disable=SC2086,SC2027
    echo -e '\E[37;31m'"\033[1m-> "$1"\033[0m" 1>&2
    tput -T linux sgr0
}

function choice () {
	# shellcheck disable=SC2086,SC2027
	echo -e '\E[37;42m'"\033[1m-> "$1"\033[0m" 1>&2
    tput -T linux sgr0
}

function usage () {
	echo "usage: ${0} \\"
	echo "	[-n <num-simulations>] [-j <num-parallel-jobs>]"
	echo "	[-s <scenarios>] [-f <iu-list-file>]"
	echo "	[-I <gcs-input-data-bucket> ] [-i <gcs-input-data-path>]"
	echo "	[-o <gcs-output-data-root>] [-O <gcs-output-data-bucket>]"
	echo "	[-r <run-title>]"
	echo "	[-L <local-output-root>]"
	exit 1
}

# convert $1 (relative filename) to absolute
function get_abs_filename() {
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

function check_and_maybe_create_dir () {
	DIR_TYPE=$1
	DIR_PATH=$2

	info "${DIR_TYPE}-dir ${DIR_PATH} doesn't exist, create?"
	select CREATE_DIR_CHOICE in yes no ; do
		case "${CREATE_DIR_CHOICE}" in
			yes)
				mkdir -p ${DIR_PATH}
				break
				;;
			*)
				error "${DIR_TYPE}-dir must be a real path to a directory"
				usage
				;;
		esac
	done
}

function check_scenarios () {
	NOT_FOUND=0
	for s in $( echo ${1//,/ } ) ; do
		SCENARIO_FILE="scenarios/scenario${s}.json"
		if [[ ! -f "${SCENARIO_FILE}" ]] ; then
			error "scenario file ${SCENARIO_FILE} not found for scenario ${s}"
			NOT_FOUND=$((NOT_FOUND + 1))
		fi
	done

	if [[ ${NOT_FOUND} -gt 0 ]] ; then
		error "not all scenario files found, exiting."
		exit 1
	fi

}

function download_iu_list () {
	info "downloading IU list from ${1}"
	IU_LIST_FILE="run/epioncho-IU-list-$( date +%Y%m%d ).txt"
	set +e
	gsutil ls "${1}" | awk -F / '{print $NF}' | cut -f 2 -d '_' | cut -f 1 -d . | sort -V > "${IU_LIST_FILE}"
	set -e
	if [[ -f "${IU_LIST_FILE}" ]] && [[ ! -s "${IU_LIST_FILE}" ]] ; then
		error "couldn't fetch IU list from ${1}"
		rm "${IU_LIST_FILE}"
		exit 1
	fi
	info "fetched IUs into ${IU_LIST_FILE}"
	
}

# set up sensible defaults
NUM_SIMULATIONS=200
NUM_PARALLEL_JOBS=default
IU_LIST_FILE="run/epioncho-IU-list-$( date +%Y%m%d ).txt"
IU_LIST_SPECIFIED=n
GCS_INPUT_DATA_BUCKET="ntd-disease-simulator-data"
GCS_INPUT_DATA_PATH="diseases/epioncho"
GCS_OUTPUT_DATA_BUCKET="ntd-flow-result-data-dev"
GCS_OUTPUT_DATA_ROOT="ntd"
RUN_TITLE=$( date +%Y%ma )
PROJECT_ROOT_DIR=$( get_abs_filename . )
KEEP_LOCAL_DATA=n

# ensure at least a 'run' directory
mkdir -p run

# read CLI options
while getopts "hn:j:s:f:I:i:kO:r:o:L:" opts ; do

	case "${opts}" in

		h)
			usage
			;;

		n)
			NUM_SIMULATIONS=${OPTARG}
			;;

		j)
			NUM_PARALLEL_JOBS=${OPTARG}
			;;

		s)
			# e.g. 1,2a,3b
			SCENARIOS=${OPTARG}
			;;

		f)
			IU_LIST_SPECIFIED=y
			IU_LIST_FILE=$( get_abs_filename "${OPTARG}" )
			if [[ ! -f "${IU_LIST_FILE}" || ! -s "${IU_LIST_FILE}" ]] ; then
				error "IU list file must be a real path to a non-empty file"
				usage
			fi
			;;

		I)
			# e.g. ntd-disease-simulator-data
			GCS_INPUT_DATA_BUCKET=${OPTARG}
			;;

		i)
			# e.g. diseases/epioncho
			GCS_INPUT_DATA_PATH=${OPTARG}
			;;

		k)
			KEEP_LOCAL_DATA=y
			;;

		O)
			# e.g. ntd-endgame-result-data
			GCS_OUTPUT_DATA_BUCKET=${OPTARG}
			;;

		r)
			# e.g. 'ntd'
			RUN_TITLE=${OPTARG}
			;;

		o)
			# e.g. 202302a
			GCS_OUTPUT_DATA_ROOT=${OPTARG}
			;;

		L)
			LOCAL_OUTPUT_ROOT=${OPTARG}
			;;

		*)
			usage
			;;

	esac

done

# default options, potentially overridden in env
DEFAULT_SCENARIOS="1,2a,2b,3a,3b,3c"
SCENARIOS="${SCENARIOS:=${DEFAULT_SCENARIOS}}"
LOCAL_OUTPUT_ROOT="${LOCAL_OUTPUT_ROOT:=$( mkdir -p ./run/output && realpath ./run/output )}"
SCENARIO_ROOT="${SCENARIO_ROOT:=$( realpath ./scenarios )}"

# check input/output directories
OUTPUT_DATA_PATH="${LOCAL_OUTPUT_ROOT}/${GCS_OUTPUT_DATA_ROOT}/${RUN_TITLE}"
if [[ ! -d "${OUTPUT_DATA_PATH}" ]] ; then
	check_and_maybe_create_dir output "${OUTPUT_DATA_PATH}"
fi

# check supplied scenarios
if [[ -z "${SCENARIOS}" ]] ; then
	error "please specify some scenarios"
	usage
else
	check_scenarios ${SCENARIOS}
fi

# check we have a valid list of IUs to run against
if [[ -f "${IU_LIST_FILE}" ]] && [[ "${IU_LIST_SPECIFIED}" = "n" ]]; then
	info "re-use existing IU list ${IU_LIST_FILE} ?"
	select REUSE_FILE_CHOICE in yes no ; do
		case "${REUSE_FILE_CHOICE}" in

			yes)
				break
				;;

			no)
				IU_LIST_FILE=""
				break
				;;

			*)
				echo "eh?"
				;;

		esac
	done
fi

# get one if not
if [[ ! -f "${IU_LIST_FILE}" ]] ; then
	IU_LIST_LOC="gs://${GCS_INPUT_DATA_BUCKET}/${GCS_INPUT_DATA_PATH}/*.hdf5"
	download_iu_list "${IU_LIST_LOC}"
fi

GCS_DESTINATION="gs://${GCS_OUTPUT_DATA_BUCKET}/${GCS_OUTPUT_DATA_ROOT}/${RUN_TITLE}"
# display info splash
info "about to run model with these settings:"
echo "- run ${NUM_SIMULATIONS} simulations for each IU"
echo "- using scenarios ${SCENARIOS}"
echo "- across ${NUM_PARALLEL_JOBS} parallel jobs"
echo "- use ID list file ${IU_LIST_FILE} ($( wc -l ${IU_LIST_FILE} | awk '{print $1}' ) IUs)"
echo "- use scenario files in directory $( realpath ./scenarios )"
echo "- write model output files to directory ${OUTPUT_DATA_PATH}"
echo "- copy local CSV output in ${OUTPUT_DATA_PATH} to GCS location ${GCS_DESTINATION}"

# confirm go-ahead
info "confirm? (enter 1 to go ahead, 2 to quit)"
select CHOICE in yes no ; do

	case "${CHOICE}" in

		yes)

			# create a log file name
			RUN_STAMP=$( date +%Y%m%d%H%M%S )
			LOG_FILE="epioncho-run-${RUN_STAMP}-output.txt"
			FINISH_FILE="epioncho-run-${RUN_STAMP}-finished.txt"

			# run the parallel job in a detached process
			nohup \
				env \
					NUM_PARALLEL_JOBS="${NUM_PARALLEL_JOBS}" \
					NUM_SIMULATIONS="${NUM_SIMULATIONS}" \
					IU_LIST_FILE="${IU_LIST_FILE}" \
					KEEP_LOCAL_DATA="${KEEP_LOCAL_DATA}" \
					RUN_STAMP="${RUN_STAMP}" \
					SCENARIOS="${SCENARIOS}" \
					SCENARIO_ROOT="${SCENARIO_ROOT}" \
					LOG_FILE="${LOG_FILE}" \
					FINISH_FILE="${FINISH_FILE}" \
					LOCAL_OUTPUT_ROOT="${LOCAL_OUTPUT_ROOT}" \
					OUTPUT_DATA_PATH="${OUTPUT_DATA_PATH}" \
					GCS_INPUT_DATA_BUCKET="${GCS_INPUT_DATA_BUCKET}" \
					GCS_INPUT_DATA_PATH="${GCS_INPUT_DATA_PATH}" \
					GCS_OUTPUT_DATA_BUCKET="${GCS_OUTPUT_DATA_BUCKET}" \
					GCS_OUTPUT_DATA_ROOT="${GCS_OUTPUT_DATA_ROOT}" \
					RUN_TITLE="${RUN_TITLE}" \
					GCS_DESTINATION="${GCS_DESTINATION}" \
				bash ./run-in-parallel.bash "${OUTPUT_DATA_PATH}" \
					2>&1 \
					> "${PROJECT_ROOT_DIR}/${LOG_FILE}" \
					< /dev/null \
					&

			# inform user
			# shellcheck disable=SC2086
			REAL_LOG_PATH=$( realpath ${PROJECT_ROOT_DIR}/${LOG_FILE} )
			info "Epioncho model is running in a detached shell."
			echo "Log output is being saved to file: ${REAL_LOG_PATH}"
			echo "When the model runs have finished, the file $( realpath ${PROJECT_ROOT_DIR} )/${FINISH_FILE} will be created."

			exit 0
			;;

		no)
			exit 1
			;;

		*)
			echo "eh?"
			;;
	esac

done
