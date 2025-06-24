#!/usr/bin/env bash

# bail out immediately if anything errors
set -euo pipefail

# convert $1 (relative filename) to absolute
function get_abs_filename() {
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

# make sure at least output-folder is specified
if [[ $# != 1 ]] ; then
	echo "usage: ${0} <output-folder-name>" >&2
	exit 1
fi

# make sure 'parallel' program is installed
if [[ -z "$( which parallel )" ]] ; then
	echo "=> error: please ensure you've installed GNU 'parallel' before running this script." >&2
	exit 1
fi

# make sure 'pipenv' program is installed
if [[ -z "$( which pipenv )" ]] ; then
	echo "=> error: please ensure you've installed 'pipenv' before running this script." >&2
	exit 1
fi

# try to read a provided run timestamp, default to now()
RUN_STAMP=${RUN_STAMP:=$( date +%Y%m%d%H%M%S )}

# work out how many jobs to run in parallel based on environment variable
NUM_PARALLEL_JOBS=${NUM_PARALLEL_JOBS:=default}
if [[ "${NUM_PARALLEL_JOBS}" = "default" ]] ; then
	JOBS_ARG=""
else
	JOBS_ARG="-j${NUM_PARALLEL_JOBS}"
fi

# read in the running ID list file from env, default to repo version
IU_LIST_FILE=${IU_LIST_FILE:=n}

if [[ ! -f "${IU_LIST_FILE}" ]] ; then
	echo "couldn't find IU list file '${IU_LIST_FILE}', exiting run-in-parallel.bash"
	exit 1
fi

# print out run-stamp info
echo "-> running Epioncho model at ${RUN_STAMP} using ${NUM_PARALLEL_JOBS} parallel jobs"

# destination below ${OUTPUT_ROOT} where the outputs should be saved, e.g. 202208b
output_folder_name="${1}"

# shellcheck disable=SC2086
echo "-> saving output into ${OUTPUT_DATA_PATH}" >&2

# run the job in parallel
# TODO include all IUs
if [[ "${PARALLELISE_BY_SCENARIO}" = 'y' ]] ; then

	echo "Parallelising by scenario"
	for IU in $( cat "${IU_LIST_FILE}" ) ; do

		# these all run individually as one process per scenario,
		# so the HDF5 only needs to be downloaded once for each IU
		REGION=${IU:0:3}
		OUTPUT_REGION_DIR="${OUTPUT_DATA_PATH}/${REGION}"
		OUTPUT_IU_DIR="${OUTPUT_REGION_DIR}/${IU}"
		HDF5_FILE="OutputVals_${IU}.hdf5"
		HDF5_FILE_GCS_LOCATION="gs://${GCS_INPUT_DATA_BUCKET}/${GCS_INPUT_DATA_PATH}/${HDF5_FILE}"
		HDF5_FILE_LOCAL_LOCATION="${OUTPUT_IU_DIR}/${HDF5_FILE}"

		# download the HDF5 file before running the model scenarios in sequence
		if [[ -f "${HDF5_FILE_LOCAL_LOCATION}" ]] ; then
			echo "HDF5 already downloaded"
		else
			echo "copying HDF5 file from GCS..."
			echo "gsutil cp ${HDF5_FILE_GCS_LOCATION} ${HDF5_FILE_LOCAL_LOCATION}"
			gsutil cp ${HDF5_FILE_GCS_LOCATION} ${HDF5_FILE_LOCAL_LOCATION}
			echo
		fi

		echo "running scenarios: ${SCENARIOS}"

		REGION=${REGION} \
		OUTPUT_IU_DIR=${OUTPUT_IU_DIR} \
		HDF5_FILE_LOCAL_LOCATION=${HDF5_FILE_LOCAL_LOCATION} \
		NUM_SIMULATIONS=${NUM_SIMULATIONS:=5} \
			parallel --line-buffer ${JOBS_ARG} -a <( echo "${SCENARIOS}" | sed 's/,/\n/g' ) \
			pipenv run bash run-epioncho-model-by-scenario.bash ${IU}

		echo "Removing HDF5 file ..."
		rm -f ${HDF5_FILE_LOCAL_LOCATION}
	done

else
	NUM_SIMULATIONS=${NUM_SIMULATIONS:=5} \
		parallel --line-buffer ${JOBS_ARG} -a <( cat ${IU_LIST_FILE} ) \
		pipenv run bash run-epioncho-model.bash
fi

# indicate completion
FINISH_STAMP=$( date +%Y%m%d%H%M%S )
echo "== Epioncho model run ${RUN_STAMP} finished at ${FINISH_STAMP}."
echo "== finished running Epioncho model at ${FINISH_STAMP}" > "${FINISH_FILE}"
