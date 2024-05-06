IU=${1}
echo python convert-sth-pickle-data.py ${IU}
for s in $( gsutil ls gs://ntd-disease-simulator-data/diseases/sth-hookworm/source-data-uploaded-20220712/${IU:0:3}/${IU}/*.csv ) ; do
    f=$( echo $s | awk -F / '{print $NF}' )
    echo "--> $f"
    gsutil cp ${s} ${s/uploaded-20220712/converted-20240505}
done
