#!/usr/bin/env bash

if [ "$#" -lt 5 ]; then
    echo "Usage: $0 [query_folder] [config_file] [result_folder] [errors_files] [Partitioning]"
    exit 1
fi

echo -e  "qname,decompositionTime,planningTime,firstResult,overallExecTime,status,cardinality" >> $3
for n in {1..5}; do
    for query in `ls -v $1/*`; do
        (timeout -s 12 300 start_experiment.py -c $2 -q $query -t $5 -s True ) 2>> $4 >> $3;
        # kill any remaining processes
        # pkill -9 start_experiment.py
        # kill -9 $(pidof start_experiment.py)
        killall -9 --quiet start_experiment.py
    done;
done;