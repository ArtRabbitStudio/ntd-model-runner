### call it as $0 sth-hookworm:sth-roundworm:sth-whipworm RwandaRuns-202307-STH.csv
### or $0 sch-mansoni RwandaRuns-202307-SCH.csv
### it'll generate e.g. iu-disease-data/rwanda-202307/sth-roundworm-IUs-rwa_high_5.csv

diseases=$( echo "${1}" | awk -F : '{for(i=1;i<=NF;i++) printf $i" "; print ""}')
scenario_csv=${2}

if [[ ! -f ${scenario_csv} ]] ; then
    echo "${scenario_csv} is not a file"
    exit 1
fi

for disease in ${diseases}; do
    for level in Low Mod High ; do
        for scenario in {1..5} ; do
            iu_csv="iu-disease-data/rwanda-202307/${disease}-IUs-rwa_${level,,}_${scenario}.csv"
            echo "----- ${level}Scen${scenario} -----"
            num_to_run=$( ag "${level}Scen${scenario}" < ${scenario_csv} | xsv select 1 2>/dev/null | wc -l )
            if [[ $num_to_run -eq 0 ]] ; then
                echo "no IU/scenario combos for ${level}Scen${scenario}"
                continue
            fi
            echo "( ${iu_csv} )"
            echo '"Group_name","IU_ID","IU_ID2"' > "${iu_csv}"
            ag "${level}Scen${scenario}" < ${scenario_csv} | xsv select 1 2>/dev/null | awk '{print "\"1\",\"" $1 "\",\"RWA" $1 "\""}' >> "${iu_csv}"
        done
    done
done
