#!/bin/bash
PIDS=$(ps -ef | grep recommendation-v2_linux_amd64 | awk '/-useDynamoDb=true/ {print $2}')

if [ -n "$PIDS" ]; then
                        kill -9 $PIDS
                        echo -e "Stopped recommendation engine PIDs ${PIDS}.\n"
                else
                        echo -e "Recommendation engine is not running.\n"
fi

