#!/bin/bash

for i in {1..100}; do
    echo "Running test iteration $i..."
    go test -run 3A -race
    if [ $? -ne 0 ]; then
        echo "Error: Test failed on iteration $i"
        exit 1
    fi
done

echo "All tests passed successfully!"