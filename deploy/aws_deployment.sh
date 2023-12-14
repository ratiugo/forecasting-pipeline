#!/bin/bash

mkdir deploy/tmp

wait_for_lambda () {
    current_state-""
    while [[ $current_state != "\"Successful\"" ]]
    do
        aws lambda get-function \
            --function-name $1 --profile $2> deploy/tmp/$1.json
        current_state=$(jq ".Configuration.LastUpdateStatus" deploy/tmp/$1.json)
        sleep 10
        echo "Waiting for $1 update to complete..."
    done
    echo "$function_name updated successfully!"
}

rm -R deploy/tmp