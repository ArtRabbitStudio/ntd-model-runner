DISEASE=${1}
IU=${2}
SOURCE_DATA_DIR=${3:-source-data}
CONVERTED_SOURCE_DATA_DIR="source-data-converted-$( date +%Y%m%d )"

python convert-sth-pickle-data.py ${DISEASE} ${IU} ${SOURCE_DATA_DIR} ${CONVERTED_SOURCE_DATA_DIR}

glob_path="gs://ntd-disease-simulator-data/diseases/sth-${DISEASE}/${SOURCE_DATA_DIR}/${IU:0:3}/${IU}"
glob="gs://ntd-disease-simulator-data/diseases/sth-${DISEASE}/${SOURCE_DATA_DIR}/${IU:0:3}/${IU}"

for s in $( gsutil ls ${glob} ) ; do
    f=$( echo $s | awk -F / '{print $NF}' )
    newname=$( echo $s | sed -e "s/${SOURCE_DATA_DIR}/${CONVERTED_SOURCE_DATA_DIR}/" )
    echo "  --> from: $f"
    echo "    --> to: $newname"
    gsutil cp ${s} "${newname}"
done

echo "==> new converted directory for ${IU}:"
new_glob_path="gs://ntd-disease-simulator-data/diseases/sth-${DISEASE}/${CONVERTED_SOURCE_DATA_DIR}/${IU:0:3}/${IU}"
gsutil ls -l "${new_glob_path}/*"
