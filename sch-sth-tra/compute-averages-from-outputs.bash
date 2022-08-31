#!/usr/bin/env bash

for n in 32 128 ; do
	for d in ascaris mansoni ; do
		echo "-> processing $n-results/$d"
		pushd $n-results/$d
		ag --no-numbers 'finished [0-9]' $(find . -type f | sort) > $n-$d-results-model.txt
		ag --no-numbers 'finished IHME' $(find . -type f | sort) > $n-$d-results-transform-ihme.txt
		ag --no-numbers 'finished IPM' $(find . -type f | sort) > $n-$d-results-transform-ipm.txt
		echo 'run	iu	type	time' > $n-$d-results.tsv
		awk '{ print $1 " " $3 " " $9 }' $n-$d-results-*.txt | sed -e 's/\// /g' -e 's/:->//g' -e 's/\.out//g' -e 's/s$//g' | sed 's/ /	/g' >> $n-$d-results.tsv
		sqlite3 <<-EOF
		.headers on
		.mode tabs
		.import $n-$d-results.tsv results
		.output $n-$d-averages.csv
		select iu, type,  round(avg(time),2) as seconds, round(avg(time) / 60,2) as minutes from results group by iu, type;
		EOF
		popd
	done
done
