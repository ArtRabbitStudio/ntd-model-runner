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
	GCS_DESTINATION \
	KEEP_LOCAL_DATA
do

    if [[ -z "${!env_var}" ]]; then
        echo "$$ xx> variable ${env_var} not set"
        exit 1
    fi

done
set -u

# check IU provided in argv
if [[ -z "${1}" ]] ; then
	echo "$$ xx> IU code not provided"
	exit 1
fi

# work out the IU and where to find the HDF5 file first
# TODO put these somewhere interesting
IU=${1}
REGION=${IU:0:3}
OUTPUT_REGION_DIR="${OUTPUT_DATA_PATH}/${REGION}"
OUTPUT_IU_DIR="${OUTPUT_REGION_DIR}/${IU}"
HDF5_FILE="OutputVals_${IU}.hdf5"
HDF5_FILE_GCS_LOCATION="gs://${GCS_INPUT_DATA_BUCKET}/${GCS_INPUT_DATA_PATH}/${HDF5_FILE}"
HDF5_FILE_LOCAL_LOCATION="${OUTPUT_IU_DIR}/${HDF5_FILE}"

function log () {
	echo "$$ ${IU} | ${1}"
}

# make sure the output dir for this region exists
mkdir -p "${OUTPUT_IU_DIR}"

# download the HDF5 file before running the model scenarios in sequence
if [[ -f "${HDF5_FILE_LOCAL_LOCATION}" ]] ; then
	log "HDF5 already downloaded"
else
	log "copying HDF5 file from GCS..."
	log "util cp ${HDF5_FILE_GCS_LOCATION} ${HDF5_FILE_LOCAL_LOCATION}"
	gsutil cp ${HDF5_FILE_GCS_LOCATION} ${HDF5_FILE_LOCAL_LOCATION}
fi
echo
log "running scenarios:"

# these all run in sequence so the parallelism is per-IU,
# and the HDF5 only needs to be downloaded once for each IU
for s in ${SCENARIOS//,/ } ; do

	SCENARIO_FILE="${SCENARIO_ROOT}/scenario${s}.json"
	CSV_OUTPUT_FILE="ihme-${IU}-scenario_${s}-${NUM_SIMULATIONS}_sims.csv"
	CSV_OUTPUT_PATH="${OUTPUT_IU_DIR}/${CSV_OUTPUT_FILE}"

	# TODO pass as parameters
	RUN_MODEL_ITERATIONS_INCLUSIVELY="true"
	PREVALENCE_OAE="true"

	log "python run.py ${HDF5_FILE_LOCAL_LOCATION} ${SCENARIO_FILE} ${CSV_OUTPUT_PATH} ${NUM_SIMULATIONS} ${RUN_MODEL_ITERATIONS_INCLUSIVELY} ${PREVALENCE_OAE}"
	python run.py ${HDF5_FILE_LOCAL_LOCATION} ${SCENARIO_FILE} ${CSV_OUTPUT_PATH} ${NUM_SIMULATIONS} ${RUN_MODEL_ITERATIONS_INCLUSIVELY} ${PREVALENCE_OAE}

	log "bzip2 -9 ${CSV_OUTPUT_PATH}"
	bzip2 -f -9 ${CSV_OUTPUT_PATH}

	log "gsutil cp ${CSV_OUTPUT_PATH}.bz2 ${GCS_DESTINATION}/epioncho/scenario_${s}/${REGION}/${IU}/${CSV_OUTPUT_FILE}.bz2"
	gsutil cp ${CSV_OUTPUT_PATH}.bz2 ${GCS_DESTINATION}/epioncho/scenario_${s}/${REGION}/${IU}/${CSV_OUTPUT_FILE}.bz2

	echo
done

if [[ "${KEEP_LOCAL_DATA}" != 'y' ]] ; then
	log "- removing data files in ${OUTPUT_IU_DIR}"
	log "rm -rf ${OUTPUT_IU_DIR}"
	rm -rf ${OUTPUT_IU_DIR}
fi
echo
