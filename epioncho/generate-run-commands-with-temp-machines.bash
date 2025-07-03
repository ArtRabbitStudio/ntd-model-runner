#!/usr/bin/env bash

SCENARIO_LIST_1="1"
SCENARIO_LIST_2="2"
SCENARIO_LIST_3="7"
SCENARIO_LIST_4="7_VC75"
SCENARIO_LIST_5="7_VC75_dynamic_stop"

DESTINATION_BUCKET="ntd-endgame-result-data"
RUN_TITLE="202503b-epioncho-february-and-august-updates"
RUN_FOLDER="epioncho-20250306-february-and-august-updates"
SCENARIO_FOLDER="epioncho-scenarios-20250306"
IU_LIST_FILENAME_ROOT="combined_red_yellow_orange_ius_needing_feb_and_aug_sims"

SOURCE_BUCKET="ntd-disease-simulator-data"
SOURCE_FOLDER="diseases/epioncho-ALL-v1-20240805"

for triplet in \
temp1:europe-west1-c:aa:1:126 \
temp2:europe-west1-c:aa:2:126 \
temp3:europe-west2-c:aa:3:126 \
temp4:europe-west2-c:aa:4:126 \
temp5:europe-west3-c:aa:5:126 \
temp6:europe-west3-c:ab:1:20 \
temp6:europe-west3-c:ab:2:20 \
temp6:europe-west3-c:ab:3:20 \
temp6:europe-west3-c:ab:4:20 \
temp6:europe-west3-c:ab:5:20 
#	temp7:europe-west4-c:ag:1 \
#	temp8:europe-west4-c:ah:1 \
#	temp9:europe-west6-c:ai:1 \
#	temp10:europe-west6-c:aj:1 \
#	temp11:europe-west8-c:ak:1 \
#	temp12:europe-west8-c:al:1 \
#	temp13:europe-west9-c:am:1 \
#	temp14:europe-west9-c:an:1 \
#	temp15:europe-west10-c:ao:1 \
#	temp16:europe-west10-c:ap:1 \
#	temp17:europe-west12-c:aq:1
do

	machine=$( echo $triplet | cut -f 1 -d : )
	zone=$( echo $triplet | cut -f 2 -d : )
	chunk=$( echo $triplet | cut -f 3 -d : )
	scenario_list_number=$( echo $triplet | cut -f 4 -d : )
	scenario_list_name="SCENARIO_LIST_${scenario_list_number}"
	cores=$( echo $triplet | cut -f 5 -d : )
	if [[ -z "${cores}" ]] ; then
		NUM_CORES=126
	else
		NUM_CORES=${cores}
	fi
	
	filename="${RUN_FOLDER}.tbz"
	file=$( realpath ~/${filename} )

	echo "------ ${machine}  |  ${zone}  |  ${chunk}  |  ${scenario_list_number} ------"
	echo
#	echo "gcloud compute instances start ${machine} --zone=${zone} --project=artrabbit-clients-ntd"
#	echo
#	echo "gcloud compute scp ${file} ${machine}:${filename} --zone=${zone} --project=artrabbit-clients-ntd"
#	gcloud compute scp ${file} ${machine}:${filename} --zone=${zone} --project=artrabbit-clients-ntd
#	echo
	echo "gcloud compute ssh ${machine} --zone=${zone} --project=artrabbit-clients-ntd"
	echo


	cat <<EOF
cd ~/ntd-model-runner/epioncho && bash ./run.bash \\
	-f ~/${RUN_FOLDER}/${IU_LIST_FILENAME_ROOT}-${chunk}.txt \\
	-S ~/${RUN_FOLDER}/${SCENARIO_FOLDER} \\
	-s ${!scenario_list_name} \\
	-n 200 \\
	-I ${SOURCE_BUCKET} \\
	-i ${SOURCE_FOLDER} \\
	-j ${NUM_CORES} \\
	-L ~/${RUN_FOLDER}/outputs \\
	-O ${DESTINATION_BUCKET} \\
	-r ${RUN_TITLE} \\
	-G \\
	-C
EOF

	echo

done

echo
echo
