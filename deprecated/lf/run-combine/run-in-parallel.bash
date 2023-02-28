#!/usr/bin/env bash

if [[ $# != 1 ]] ; then
	echo "usage: ${0} <output-folder-name>"
	exit 1
fi

# destination below 'combined_output/ntd' where the outputs should be saved, e.g. 202208b
output_folder_name="${1}"

# work out how many jobs to run
NUM_JOBS=${NUM_JOBS:=default}
if [[ "${NUM_JOBS}" = "default" ]] ; then
	JOBS_ARG=""
else
	JOBS_ARG="-j${NUM_JOBS}"
fi

RUNNING_ID_LIST_FILE=${RUNNING_ID_LIST_FILE:=running-id-list.txt}

# run the job
NUM_SIMULATIONS=${NUM_SIMULATIONS:=5} time parallel ${JOBS_ARG} -a "${RUNNING_ID_LIST_FILE}" bash run-lf-model.bash "${output_folder_name}"
