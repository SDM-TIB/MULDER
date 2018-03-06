#!/usr/bin/env bash

if [ "$#" -lt 5 ]; then
    echo "Usage: $0 [query_folder] [config_file] [result_folder] [errors_files] [Partitioning]"
    exit 1
fi

#echo -e  "qname\tdecompositionTime\tplanningTime\tfirstResult\toverallTime\tmoreResults\tcardinality" >> $3

for query in `ls -v $1/*`; do
    (timeout -s 12 300 start_dief_experiment.py -c $2 -q $query -r $3 -t $5 -s True ) >> $4;

done;

#(timeout -s 12 300 start_experiment.py -c $2 -q $query -t $5 -s True ) 2>> $4 >> $3;
#-t SemEP  or -t MULDER or -t METIS
