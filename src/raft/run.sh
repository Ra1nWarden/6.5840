#!/bin/bash

for i in {1..10}; do
    echo "Running test iteration $i..."
    go test -run 3B -race
    if [ $? -ne 0 ]; then
        echo "Error: Test failed on iteration $i"
        exit 1
    fi
done

echo "All tests passed successfully!"