fit=$1
for uri in $( cat "trachoma-source-data-20220830a-${fit}-listing.txt" )
do
	file=$( echo $uri | awk -F / '{print $7}' )
	iu=$( echo ${file} | cut -f 2 -d _ | cut -f 1 -d . )
	region=${iu:0:3}
	new_uri="gs://ntd-disease-simulator-data/diseases/trachoma/source-data-20220830-${fit}/${region}/${iu}/${file}"
	gsutil -m cp ${uri} ${new_uri}
	exit
done
