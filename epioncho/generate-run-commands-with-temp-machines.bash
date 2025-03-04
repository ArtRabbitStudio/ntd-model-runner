#!/usr/bin/env bash
for disease in Tra:trachoma:trachoma ; do
#for disease in Tri:whipworm:trichuris ; do
#for disease in Asc:roundworm:ascaris ; do
#for disease in Hook:hookworm:hookworm ; do

SCENARIO_LIST_1="7_VC75_dynamic_stop,7_VC75_dynamic_stop_immigration"

for triplet in \
temp1:europe-west1-c:aa:1 \
temp2:europe-west1-c:ab:1 \
temp3:europe-west2-c:ac:1 \
temp4:europe-west2-c:ad:1 \
temp5:europe-west3-c:ae:1 \
temp6:europe-west3-c:af:1 \
temp7:europe-west4-c:ag:1 \
temp8:europe-west4-c:ah:1 \
temp9:europe-west6-c:ai:1 \
temp10:europe-west6-c:aj:1  \
epioncho:europe-west1-b:ak:1 \
epioncho2:europe-west3-b:al:1
do

		machine=$( echo $triplet | cut -f 1 -d : )
		zone=$( echo $triplet | cut -f 2 -d : )
		chunk=$( echo $triplet | cut -f 3 -d : )
		scenario_list_number=$( echo $triplet | cut -f 4 -d : )
		scenario_list_name="SCENARIO_LIST_${scenario_list_number}"
		
		filename="epioncho-20250218-all.tbz"
		file=$( realpath ~/${filename} )

		echo "------ ${machine}  :  ${zone}  :  ${chunk}  :  ${scenario_list_number} ------"
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
    -f /home/igor/epioncho-20250303-all/epioncho-iu-list-long-codes-20250303-${chunk}.txt \\
    -S /home/igor/epioncho-20250303-all/epioncho-scenarios-20250303 \\
    -s ${!scenario_list_name} \\
    -n 200 \\
    -I ntd-disease-simulator-data \\
    -i diseases/epioncho-ALL-v1-20240805 \\
    -j 126 \\
    -L ~/epioncho-20250303-all \\
    -O ntd-endgame-result-data \\
    -r 202503a-epioncho-all \\
    -G \\
    -C
~
EOF

		echo



	done
    echo
    echo
done
