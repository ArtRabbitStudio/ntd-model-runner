#!/usr/bin/env bash
# shellcheck disable=SC2086

# default options, potentially overridden in env
PARAMETER_ROOT="${PARAMETER_ROOT:=./parameters}"
SCENARIO_ROOT="${SCENARIO_ROOT:=./scenarios}"
RESULTS_ROOT="${RESULTS_ROOT:=./results}"
NUM_SIMULATIONS=${NUM_SIMULATIONS:=5}

# run the model for a given ID, provided as $2 and passed in at bottom
function run_ID () {

	output_folder_name=${1}
	id=${2}

	PARAMS="${PARAMETER_ROOT}/RandomParamIU${id}.txt"
	SCENARIO="${SCENARIO_ROOT}/scenariosNoImp${id}.xml"
	RESULTS="${RESULTS_ROOT}/${id}"

	echo "== making result directory ${RESULTS} for ID ${id}"

	mkdir -p "${RESULTS}"

	echo "== running ${NUM_SIMULATIONS} simulations of LF model with ${PARAMS} ${SCENARIO}"
	transfil_N \
		-p  "${PARAMS}" \
		-s  "${SCENARIO}" \
		-o "${RESULTS}" \
		-n ./Pop_Distribution.csv \
		-r "${NUM_SIMULATIONS}"

	echo "== converting output files for IHME & IPM"
	( time do_file_conversions "${id}" "${output_folder_name}" ) 2>&1

	echo "== clearing out model 'result' files"
	rm -rf "${RESULTS}"
}

function do_file_conversions () {

	id=${1}
	output_folder_name=${2}

	for scen_iu in $( xmllint --xpath "/Model/ScenarioList/scenario/@name" <( tail -n +2 scenarios/scenariosNoImp${id}.xml ) | sed 's/name="\([^"]*\)"/\1/g' ) ; do

		scen=$( echo $scen_iu | cut -f 1 -d _ )
		iu=$( echo $scen_iu | cut -f 2 -d _ )

		convert_output_files "${scen}" "${iu}" IHME "${output_folder_name}"
		convert_output_files "${scen}" "${iu}" IPM "${output_folder_name}"

	done

	echo "== done converting output files"
}

function convert_output_files () {

	scen=${1}
	iu=${2}
	inst=${3^^}
	output_folder_name=${4}

	echo "== converting ${inst} files for IU ${iu} scenario ${scen}"
	last_tmp=$( mktemp )

	MODEL_OUTPUT_FILE_ROOT=res_endgame/${inst}_scen${scen}/${scen}_${iu}

	# shellcheck disable=SC2004
	for n in $(seq 0 $(( $NUM_SIMULATIONS - 2 )) ); do

		NEXT_SEQUENCE_NUMBER=$(( n + 1 ))

		if [[ $n -eq 0 ]] ; then
			PASTE_FILE_A=${MODEL_OUTPUT_FILE_ROOT}/${inst}_scen${scen}_${iu}_rep_${n}.csv
		else
			PASTE_FILE_A=${last_tmp}
		fi

		PASTE_FILE_B=${MODEL_OUTPUT_FILE_ROOT}/${inst}_scen${scen}_${iu}_rep_${NEXT_SEQUENCE_NUMBER}.csv

		new_tmp=$( mktemp )

		case ${inst} in

			IHME)
				# take each whole line from the model output
				paste -d , ${PASTE_FILE_A} <( sed "s/draw_0$/draw_${NEXT_SEQUENCE_NUMBER}/g" < ${PASTE_FILE_B} ) > ${new_tmp}
				;;

			IPM)
				# take only the last field from the model output
				paste -d , ${PASTE_FILE_A} <( awk -F , '{print $NF}' < ${PASTE_FILE_B} | sed "s/draw_0$/draw_${NEXT_SEQUENCE_NUMBER}/g" ) > ${new_tmp}
				;;

			*)
				echo "xx> unknown institute ${inst}"
				;;

		esac

		rm ${last_tmp}
		last_tmp=${new_tmp}

	done

	organise_output_files "${scen}" "${iu}" "${inst}" "${output_folder_name}"

}

function organise_output_files () {

	scen=${1}
	iu=${2}
	inst=${3^^}
	output_folder_name=${4}

	IU_OUTPUT_PATH="ntd/${output_folder_name}/lf/scenario_${scen}/${iu}"

	LOCAL_IU_OUTPUT_DIR="combined_output/${IU_OUTPUT_PATH}"
	LOCAL_IU_OUTPUT_FILE_NAME="${inst,,}-${iu}-lf-scenario_${scen}-${NUM_SIMULATIONS}.csv"
	LOCAL_IU_OUTPUT_FILE_PATH="${LOCAL_IU_OUTPUT_DIR}/${LOCAL_IU_OUTPUT_FILE_NAME}"
#	LOCAL_IU_OUTPUT_FILE_PATH_BZ="${LOCAL_IU_OUTPUT_DIR}/${LOCAL_IU_OUTPUT_FILE_NAME}.bz2"

#	GCS_IU_OUTPUT_DIR="gs://ntd-endgame-result-data/${IU_OUTPUT_PATH}"
#	GCS_IU_OUTPUT_FILE_NAME="${LOCAL_IU_OUTPUT_FILE_NAME}.bz2"
#	GCS_IU_OUTPUT_FILE_PATH="${GCS_IU_OUTPUT_DIR}/${GCS_IU_OUTPUT_FILE_NAME}"

	mkdir -p "${LOCAL_IU_OUTPUT_DIR}"
	mv $last_tmp ${LOCAL_IU_OUTPUT_FILE_PATH}

	# clear out intermediate files in e.g. res_endgame/IPM_scen3c/3c_SDN53289
#	rm -rf ${MODEL_OUTPUT_FILE_ROOT}

	# remove IU folders e.g. ./res_endgame/IPM_scen3b/3b_TCD10760
	# shellcheck disable=SC2044
	for s in $( find ./res_endgame -type d -empty ) ; do
		if [[ -d "${s}" ]] ; then
			rm -rf "${s}"
		fi
	done

	# strip out the scenario number from the IU in the output
	gsed -i "s/${scen}_${iu}/${iu}/g" ${LOCAL_IU_OUTPUT_FILE_PATH}

	# compress the files
	bzip2 --force --best ${LOCAL_IU_OUTPUT_FILE_PATH}

}

if [[ $# != 2 ]] ; then
	echo "usage: ${0} <output-folder> <running-ID>"
	exit 1
fi

run_ID "${1}" "${2}"
