#!/bin/bash

COMMAND="go test -run 3A -race"

for i in {1..10}
    do
        eval "$COMMAND > ./logs/log$i"
    done