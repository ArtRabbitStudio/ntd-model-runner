#!/usr/bin/env bash
for disease in Tra:trachoma:trachoma ; do
#for disease in Tri:whipworm:trichuris ; do
#for disease in Asc:roundworm:ascaris ; do
#for disease in Hook:hookworm:hookworm ; do
	for pair in sch:europe-west3-b:aa sch2:europe-west6-b:ab sch3:europe-west6-b:ac sch4:europe-west8-b:ad epioncho:europe-west1-b:ae epioncho2:europe-west3-b:af epioncho3:europe-west4-b:ag epioncho4:europe-west2-b:ah epioncho5:europe-west4-b:ai lf-backup:europe-west1-b:aj ; do

		machine=$( echo $pair | cut -f 1 -d : )
		zone=$( echo $pair | cut -f 2 -d : )
		chunk=$( echo $pair | cut -f 3 -d : )
		
		filename="epioncho-20250204-all.tbz"
		file=$( realpath ~/${filename} )

		echo "------ ${machine}:${zone}:${chunk} ------"
		echo
		echo "gcloud compute instances start ${machine} --zone=${zone} --project=artrabbit-clients-ntd"
		echo
		echo "gcloud compute scp ${file} ${machine}:${filename} --zone=${zone} --project=artrabbit-clients-ntd"
	#	gcloud compute scp ${file} ${machine}:${filename} --zone=${zone} --project=artrabbit-clients-ntd
		echo

		scenario_list="15_VC50,15_VC75,1_VC50,1_VC75,2_VC50,2_VC75,7_VC50,7_VC75,8_VC50,8_VC75"

		cat <<EOF
cd ~/ntd-model-runner/epioncho && bash ./run.bash \\
	-f /home/igor/epioncho-20250204-all/epioncho-iu-list-long-codes-20250204-${chunk}.txt \\
	-S /home/igor/epioncho-20250204-all/scenarios \\
	-s ${scenario_list} \\
	-n 200 \\
	-I ntd-disease-simulator-data \\
	-i diseases/epioncho-ALL-v1-20240805 \\
	-j 126 \\
	-L ~/epioncho-20250204-all/outputs \\
	-O ntd-endgame-result-data \\
	-r 202502b-epioncho-all \\
	-G \\
	-C
EOF

		echo



	done
    echo
    echo
done
