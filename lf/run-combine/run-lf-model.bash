PARAMETER_ROOT=/Users/igor/Work/ntd/ntd-models/ntd-model-runner/lf/parameters
SCENARIO_ROOT=/Users/igor/Work/ntd/ntd-models/ntd-model-runner/lf/scenarios
RESULTS_ROOT=./results
NUM_SIMULATIONS=20

#for id in $( cat running-id-list.txt ) ; do
for id in 1 ; do

	PARAMS="${PARAMETER_ROOT}/RandomParamIU${id}.txt"
	SCENARIO="${SCENARIO_ROOT}/scenariosNoImp${id}.xml"
	RESULTS="${RESULTS_ROOT}/${id}"

	echo "making result directory ${RESULTS}"

	mkdir -p "${RESULTS}"

	transfil_N \
		-p  "${PARAMS}" \
		-s  "${SCENARIO}" \
		-o "${RESULTS}" \
		-n ./Pop_Distribution.csv \
		-r ${NUM_SIMULATIONS}

	for scen_iu in $( xmllint --xpath "/Model/ScenarioList/scenario/@name" <( tail -n +2 scenarios/scenariosNoImp${id}.xml ) | sed 's/name="\([^"]*\)"/\1/g' ) ; do

		echo "---> $scen_iu"

		scen=$( echo $scen_iu | cut -f 1 -d _ )
		iu=$( echo $scen_iu | cut -f 2 -d _ )

		last_tmp=$( mktemp )

		for n in $(seq 0 $(( $NUM_SIMULATIONS - 2)) ); do

			IHME_FILE_ROOT=res_endgame/IHME_scen${scen}/${scen_iu}

			if [[ $n -eq 0 ]] ; then
				PASTE_FILE_A=${IHME_FILE_ROOT}/IHME_scen${scen_iu}_rep_${n}.csv
			else
				PASTE_FILE_A=${last_tmp}
			fi

			PASTE_FILE_B=${IHME_FILE_ROOT}/IHME_scen${scen_iu}_rep_$(( n + 1 )).csv

			new_tmp=$( mktemp )

			paste -d , $PASTE_FILE_A <( sed "s/draw_0$/draw_$(( n + 1 ))/g" < $PASTE_FILE_B ) > ${new_tmp}

			rm ${last_tmp}
			last_tmp=${new_tmp}

		done

		echo $last_tmp
		IHME_OUTPUT_FILE=${IHME_FILE_ROOT}/IHME_scen${scen_iu}_output.csv
		mv $last_tmp ${IHME_OUTPUT_FILE}
		gsed -i "s/${scen_iu}/${iu}/g" ${IHME_OUTPUT_FILE}
		echo "---> output file: ${IHME_OUTPUT_FILE}"

		exit

	done
	
done
