#!/usr/bin/env bash

set -euo pipefail

echo "-> process $$ running $@"
python check-python-env.py

# check appropriate env variables have been set (in run.bash)
set +u
for env_var in \
	NUM_SIMULATIONS \
	SCENARIOS \
	SCENARIO_ROOT \
	OUTPUT_DATA_PATH \
	LOCAL_OUTPUT_ROOT \
	GCS_INPUT_DATA_BUCKET \
	GCS_INPUT_DATA_PATH \
	GCS_DESTINATION
do

    if [[ -z "${!env_var}" ]]; then
        echo "xx> variable ${env_var} not set"
        exit 1
    fi

done
set -u

# check IU provided in argv
if [[ -z "${1}" ]] ; then
	echo "xx> IU code not provided"
	exit 1
fi

# work out the IU and where to find the HDF5 file first
# TODO put these somewhere interesting
IU=${1}
REGION=${IU:0:3}
HDF5_FILE="OutputVals_${IU}.hdf5"
HDF5_FILE_GCS_LOCATION="gs://${GCS_INPUT_DATA_BUCKET}/${GCS_INPUT_DATA_PATH}/${HDF5_FILE}"
HDF5_FILE_LOCAL_LOCATION="${OUTPUT_DATA_PATH}/${HDF5_FILE}"

# download the HDF5 file before running the model scenarios in sequence
echo "- copying HDF5 file from GCS..."
echo "$$ gsutil cp ${HDF5_FILE_GCS_LOCATION} ${HDF5_FILE_LOCAL_LOCATION}"
 #gsutil cp ${HDF5_FILE_GCS_LOCATION} ${HDF5_FILE_LOCAL_LOCATION}
echo
echo "- running scenarios:"

# these all run in sequence so the parallelism is per-IU,
# and the HDF5 only needs to be downloaded once for each IU
for s in ${SCENARIOS//,/ } ; do

	SCENARIO_FILE="${SCENARIO_ROOT}/scenario${s}.json"
	CSV_OUTPUT_FILE="ihme-${IU}-scenario_${s}-${NUM_SIMULATIONS}.csv"
	CSV_OUTPUT_PATH="${OUTPUT_DATA_PATH}/${CSV_OUTPUT_FILE}"

	echo "$$ python run.py ${HDF5_FILE_LOCAL_LOCATION} ${SCENARIO_FILE} ${CSV_OUTPUT_PATH} ${NUM_SIMULATIONS}"
	# python run.py ${HDF5_FILE_LOCAL_LOCATION} ${SCENARIO_FILE} ${CSV_OUTPUT_PATH} ${NUM_SIMULATIONS}

	echo "$$ bzip2 -9 ${CSV_OUTPUT_PATH}"
	# bzip2 -9 ${CSV_OUTPUT_PATH}

	echo "$$ gsutil cp ${CSV_OUTPUT_PATH}.bz2 ${GCS_DESTINATION}/epioncho/scenario_${s}/${REGION}/${IU}/${CSV_OUTPUT_FILE}.bz2"
	# gsutil cp ${CSV_OUTPUT_PATH}.bz2 ${GCS_DESTINATION}/epioncho/scenario_${s}/${REGION}/${IU}/${CSV_OUTPUT_FILE}.bz2

	echo "$$ rm ${CSV_OUTPUT_PATH}.bz2"
	# rm ${CSV_OUTPUT_PATH}.bz2

	echo
done

echo "- removing HDF5 file"
echo "$$ rm ${HDF5_FILE_LOCAL_LOCATION}"
# rm "${HDF5_FILE_LOCAL_LOCATION}"
echo
