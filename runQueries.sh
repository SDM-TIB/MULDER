#!/usr/bin/env bash

if [ "$#" -lt 6 ]; then
    echo "Usage: $0 [query_folder] [config_file] [result_file_name] [errors_file_name] [planonlyTrueorFalse]"
    exit 1
fi

echo -e  "qname,decompositionTime,planningTime,firstResult,overallExecTime,status,cardinality" >> $3
#for n in {1..5}; do
for query in `ls -v $1/*`; do

    (timeout -s 12 300 start_experiment.py -c $2 -q $query -t MULDER -s True -p $5 ) 2>> $4 >> $3;

    # kill any remaining processes
    killall -9 --quiet start_experiment.py
done;
#done;