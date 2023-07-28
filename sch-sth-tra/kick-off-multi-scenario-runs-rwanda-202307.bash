disease_long=${1}
disease_short=${2}
output_bucket=${3}

num_sims=${NUM_SIMS:-200}
dry_run=${DRY_RUN:-n}

for file in iu-disease-data/rwanda-202307/${disease_long}-IUs-*.csv ; do
    echo $file
    scenario=$( basename $file | cut -f 4 -d '-' | cut -f 1 -d . )
    DISPLAY_CMD=${dry_run} FORCE_ADD_TO_EXISTING_RUN=y DONT_WRITE_DB_RECORD=y bash run-sch-sth-tra.bash -s ${scenario} -i ${file} -n ${num_sims} -d ${disease_short} -p source-data-rwanda-fitting-202307a -k ntd-disease-simulator-data -K ${output_bucket} -N "202307d-rwanda" -o 202307d-rwanda -e igor@artrabbit.com
done


