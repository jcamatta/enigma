#!bin/bash

project=""
datasets=$(bq ls --datasets $project | tail -n +3)


mkdir -p ./table_schemas

for dataset in $datasets; do

    tables=$(bq ls ${project}:${dataset} | tail -n +3 | awk '{print $1}') 
    
    if [[ -z "${tables}" ]]; then
        continue
    fi

    mkdir -p ./table_schemas/$dataset
    for table in $tables; do

        if [[ -z "${table}" ]]; then
            continue
        fi

        table_id=${project}:${dataset}.$table
        bq show --format=prettyjson --schema=True $table_id | python -m json.tool > ./table_schemas/${dataset}/${table}.json
        
    done
    break
done