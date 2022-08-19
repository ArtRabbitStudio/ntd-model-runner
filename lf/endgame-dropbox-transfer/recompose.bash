#!/usr/bin/env bash

# make sure 'ag' is installed
if [[ $( which ag ) == "" ]] ; then
	sudo apt-get install -y silversearcher-ag
fi

# set up local & remote files & paths
MUNGED2="url-list-2-res_munged2.txt"
MDA_TABLE_ID_CSV="MDAtableIU.csv"
GCS_PATH_ROOT="gs://ntd-endgame-result-data/ntd/202208a/lf"

# check through all the entries in RandomParamIDs.txt
for RUNNING_ID in $(cat random-param-id-lines.txt); do

	# check for all LF scenarios
	for SCEN in 1 2 3a 3b 3c ; do

		# check for both institution's files
		for INST in IHME IPM ; do

			echo "=== checking ${RUNNING_ID} ${SCEN} ${INST} ==="

			dropbox_file="${INST}_scen${SCEN}_IU_${RUNNING_ID}.csv"

			# check if this file was in the 'res_munged2' Dropbox folder listing
			ag_result=$( ag --no-numbers "${dropbox_file}" "${MUNGED2}" )

			case $? in
				
				# yes it was
				0)
					# echo "=> running ID ${RUNNING_ID} found in res_munged_2: ${ag_result}"
					DROPBOX_URL="${ag_result}?dl=0&rlkey=ar0e722j4x4f7xqalkfgehumm"
					;;

				# no it wasn't
				1)
					
					# echo "-> running ID ${RUNNING_ID} not found in res_munged2, must be in res_munged"
					DROPBOX_URL="https://www.dropbox.com/scl/fo/tls586dysvucttm4m0cno/h/${dropbox_file}?dl=0&rlkey=h77fi0d1vtzj3i4uhk4z17de9"
					;;

				# something went wrong
				2)
					echo "-> error 2 trying to find ${dropbox_file} in ${MUNGED2}"
					# log mishap to stderr
					echo "-> error 2 trying to find ${dropbox_file} in ${MUNGED2}" >&2
					echo
					continue
					;;

			esac

			# check whether the expected file exists on dropbox
			echo "-- checking ${DROPBOX_URL}"
			response=$( curl -sIL "${DROPBOX_URL}" )
			found_file_name=$( echo "${response}" | grep -E 'content-disposition: ' | awk -F '"' '{print $2}')

			# no it wasn't
			if [[ "${found_file_name}" != "${dropbox_file}" ]] ; then
				echo "-> something went wrong checking the Dropbox URL, looking for ${dropbox_file} but found '${found_file_name}'"
				# log mishap to stderr
				echo "-> ${dropbox_file} not found on dropbox" >&2
				echo
				continue
			fi

			echo "-- found ${found_file_name} on Dropbox"

			# look up running ID in MDA table to get IU code
			MDA=$( ag --no-numbers "^${RUNNING_ID}," ${MDA_TABLE_ID_CSV} )

			case $? in

				0)
					ACTUAL_IU=$( echo $MDA | awk -F , '{print $2}' | tr -d '"' )
					echo "=> found IU ${ACTUAL_IU} in MDA table" 
					;;

				1)
					echo "-> running ID ${RUNNING_ID} not found in ${MDA_TABLE_ID_CSV}"
					echo "-> running ID ${RUNNING_ID} not found in ${MDA_TABLE_ID_CSV}" >&2
					echo
					continue
					;;

				2)
					echo "-> error 2 trying to find ${RUNNING_ID} in ${MDA_TABLE_ID_CSV}"
					# log mishap to stderr
					echo "-> error 2 trying to find ${RUNNING_ID} in ${MDA_TABLE_ID_CSV}" >&2
					echo
					continue
					;;

			esac

			# put together GCS & compressed file names/paths
			GCS_FILENAME="${INST,,}-${ACTUAL_IU}-lf-scenario_${SCEN}-200_simulations.csv"
			BZIPPED_FILE="${GCS_FILENAME}.bz2"
			GCS_FILE_PATH="${GCS_PATH_ROOT}/scenario_${SCEN}/${ACTUAL_IU}/${BZIPPED_FILE}"

			# ignore this one if it's already in GCS
			echo "-- checking GCS for ${GCS_FILE_PATH}"
			found_gs_object=$( gsutil ls "${GCS_FILE_PATH}" 2>/dev/null )

			if [[ $? == 0 ]] ; then
				echo "=> file ${GCS_FILENAME} already exists in GCS, skipping"
				echo
				continue
			fi

			# download from dropbox
			echo "-- downloading file for ${RUNNING_ID}/${ACTUAL_IU} to ${GCS_FILENAME} ..."
			curl -sL -o "${GCS_FILENAME}" "${DROPBOX_URL}"

			if [[ ! -s "${GCS_FILENAME}" ]] ; then
				echo "-> error downloading from url ${DROPBOX_URL}: $?"
				echo
				continue
			fi

			# compress
			echo "-- bzipping file ${GCS_FILENAME}"
			bzip2 -9 "${GCS_FILENAME}"

			# upload to GCS
			echo "-- uploading ${BZIPPED_FILE} to ${GCS_FILE_PATH}"
			gsutil cp "${BZIPPED_FILE}" "${GCS_FILE_PATH}" 2>&1

			# clear it out
			echo "-- removing ${BZIPPED_FILE}"
			rm "${BZIPPED_FILE}"

			echo

		done


	done

done
