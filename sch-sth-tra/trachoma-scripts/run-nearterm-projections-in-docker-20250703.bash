#!/usr/bin/env bash

# with
#
#   /mnt/gcs/ntd-disease-simulator-data/nearterm-projections/trachoma/20250702
#
# mounted as
#
#   /ntdmc/trachoma-amis-integration/projections/artefacts
#
# and with folder-id specified as `nearterm`, the container saved the results into:
#
# gs://ntd-disease-simulator-data/nearterm-projections/trachoma/20250702/trachoma/nearterm/BFA/BFA05332/Trachoma_BFA05332.p
#																				 ^^^^^^^^
#
# so that means we need to mount ntd-disease-simulator-data/nearterm-projections
# onto `artefacts` and specify folder-id as nearterm-20250703
#
# and that should save the results into:
#
# gs://ntd-disease-simulator-data/nearterm-projections/trachoma/nearterm-20250703

REGISTRY_PREFIX="gcr.io/artrabbit-clients-ntd"
DOCKER_IMAGE_NAME=${DOCKER_IMAGE_NAME:=trachoma-amis-pipeline}
DOCKER_IMAGE_TAG=${DOCKER_IMAGE_TAG:=20250702}
DOCKER_IMAGE="${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG}"
TARGET_FOLDER="nearterm-20250703"
NUM_CORES=${NUM_CORES:=126}
START=${1:-1}
END=${2:-1}

echo "-> setting up GCS folder mount"
sudo gcsfuse \
    -o allow_other \
    --file-mode 666 \
    --dir-mode 777 \
    --implicit-dirs \
    ntd-disease-simulator-data \
    /mnt/gcs/ntd-disease-simulator-data

if [[ -z $( docker images | grep "${REGISTRY_PREFIX}/${DOCKER_IMAGE_NAME}" | grep "${DOCKER_IMAGE_TAG}" ) ]] ; then
    echo "-> fetching latest Docker image"
    docker pull "${REGISTRY_PREFIX}/${DOCKER_IMAGE}"
fi

echo "Running batches ${START}-${END} across ${NUM_CORES} cores using image '${DOCKER_IMAGE}'"

for batch_id in $( seq ${START} ${END} ) ; do

	docker run \
		--rm \
        -v "/mnt/gcs/ntd-disease-simulator-data/diseases/trachoma/source-data-20250605-espen:/ntdmc/trachoma-amis-integration/projections-prep/artefacts/projections/trachoma/${TARGET_FOLDER}" \
        -v "/mnt/gcs/ntd-disease-simulator-data/nearterm-projections:/ntdmc/trachoma-amis-integration/projections/artefacts" \
		-ti ${REGISTRY_PREFIX}/${DOCKER_IMAGE} \
		--id=${batch_id} \
		--folder-id=${TARGET_FOLDER} \
		--stage=nearterm-projections \
		--stop-importation \
		--num-cores=${NUM_CORES}

done

