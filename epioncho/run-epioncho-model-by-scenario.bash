#!/usr/bin/env bash

set -euo pipefail

echo "-> process $$ running $@"
python check-python-env.py

# check appropriate env variables have been set (in run.bash)
set +u
for env_var in \
	REGION \
	NUM_SIMULATIONS \
	SCENARIOS \
	SCENARIO_ROOT \
	OUTPUT_DATA_PATH \
	OUTPUT_IU_DIR \
	LOCAL_OUTPUT_ROOT \
	HDF5_FILE_LOCAL_LOCATION \
	GCS_INPUT_DATA_BUCKET \
	GCS_INPUT_DATA_PATH \
	GCS_DESTINATION \
	KEEP_LOCAL_DATA \
	SAMPLING_INTERVAL \
	SHORTEN_IU_CODE \
	RUN_GROUPED
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

if [[ -z "${2}" ]] ; then
	echo "$$ xx> scenario id not provided"
	exit 1
fi

# work out the IU
# TODO put these somewhere interesting
IU=${1}

# get the scenario arg passed in by parallel
SCENARIO=${2}

function log () {
	STAMP=$( date +%H:%M:%S )
	echo "$$ ${IU} ${STAMP} | ${1}"
}

log "starting run for iu: ${IU} scenario: ${SCENARIO}"

# make sure the output dir for this region exists
mkdir -p "${OUTPUT_IU_DIR}"


	SCENARIO_FILE="${SCENARIO_ROOT}/scenario${SCENARIO}.json"
	CSV_OUTPUT_FILE="ihme-${IU}-scenario_${SCENARIO}-${NUM_SIMULATIONS}_sims.csv"
	CSV_OUTPUT_PATH="${OUTPUT_IU_DIR}/${CSV_OUTPUT_FILE}"

	# TODO pass as parameters
	RUN_MODEL_ITERATIONS_INCLUSIVELY="true"
	PREVALENCE_OAE="true"

	RUN_GROUPED=${RUN_GROUPED:=n}
	SAMPLING_INTERVAL=${SAMPLING_INTERVAL:=1}

	# choose the appropriate model runner, which takes CSV_OUTPUT_PATH as:
	if [[ "${RUN_GROUPED}" = "y" ]] ; then
		# an output file root (it strips the '.csv')
		PYTHON_FILE=run_grouped.py
	else
		# an output file path (it writes the CSV directly)
		PYTHON_FILE=run.py
	fi

	# run the model
	log "python ${PYTHON_FILE} ${HDF5_FILE_LOCAL_LOCATION} ${SCENARIO_FILE} ${CSV_OUTPUT_PATH} ${NUM_SIMULATIONS} ${RUN_MODEL_ITERATIONS_INCLUSIVELY} ${PREVALENCE_OAE} ${SAMPLING_INTERVAL}"
	python ${PYTHON_FILE} ${HDF5_FILE_LOCAL_LOCATION} ${SCENARIO_FILE} ${CSV_OUTPUT_PATH} ${NUM_SIMULATIONS} ${RUN_MODEL_ITERATIONS_INCLUSIVELY} ${PREVALENCE_OAE} ${SAMPLING_INTERVAL}

	# grouping takes the CSV path as an output root and generates multiple CSV files
	if [[ "${RUN_GROUPED}" = "y" ]] ; then
		# insert a '*' before '.csv' suffix
		CSV_OUTPUT_PATH="${CSV_OUTPUT_PATH/.csv/}*.csv"
	fi

	# bzip up the data files
	log "bzip2 -f -9 ${CSV_OUTPUT_PATH}"
	bzip2 -f -9 ${CSV_OUTPUT_PATH}

	# create GCS paths for the files
	for CSV_FILE_PATH in $( ls -1 ${CSV_OUTPUT_PATH}.bz2 2>/dev/null ) ; do

		CSV_FILE_NAME=$( echo ${CSV_FILE_PATH} | awk -F / '{print $NF}' )
		GCS_URL_PATH=${GCS_DESTINATION}/epioncho/scenario_${SCENARIO}/${REGION}/${IU}/${CSV_FILE_NAME}

		# convert BEN0036703212 to BEN03212?
		if [[ "${SHORTEN_IU_CODE}" = 'y' ]] ; then
			SHORT_IU="${IU:0:3}${IU:8:5}"
			log "converting long IU code ${IU} to ${SHORT_IU} for GCS URL..."
			GCS_URL_PATH=$( echo "${GCS_URL_PATH}" | sed -e "SCENARIO/${IU}/${SHORT_IU}/g" )
			log "conversion done: $( echo ${GCS_URL_PATH} | awk -F / '{print $NF}' )"
		fi

		log "gsutil cp ${CSV_FILE_PATH} ${GCS_URL_PATH}"
		gsutil cp ${CSV_FILE_PATH} ${GCS_URL_PATH}

	done

	echo

if [[ "${KEEP_LOCAL_DATA}" != 'y' ]] ; then
	log "- removing data files in ${OUTPUT_IU_DIR}"
	find ${OUTPUT_IU_DIR} -type f -name "*${IU}-scenario_${SCENARIO}*.csv.bz2"
	find ${OUTPUT_IU_DIR} -type f -name "*${IU}-scenario_${SCENARIO}*.csv.bz2" -exec rm -f {} \;
fi
#echo
