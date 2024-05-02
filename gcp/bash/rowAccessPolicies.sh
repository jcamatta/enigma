token=$(gcloud auth print-access-token)
project=""

echo "[" > temp.json
for dataset in $(bq ls --datasets=true $project | tail -n +3); do
    for table in $(bq ls "${project}:${dataset}" | tail -n +3 | awk '{print $1}'); do

        data=$(curl -H "Authorization: Bearer ${token}" \
        https://bigquery.googleapis.com/bigquery/v2/projects/${project}/datasets/${dataset}/tables/${table}/rowAccessPolicies)
    
        if [ "$(echo $data | jq 'length > 0')" == "true" ]; then
            echo $data | jq '' >> temp.json
            echo "," >> temp.json
        else
            echo "table $dataset.$table no contiene row_access_policies"
        fi
    done
done
cat temp.json | sed '$ s/.$//' > rowAccessPolicies.json
echo "]" >> rowAccessPolicies.json
rm temp.json