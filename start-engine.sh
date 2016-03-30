#!/bin/bash
nohup ./recommendation-v2_linux_amd64 -useDynamoDb=true -log_dir=/home/ec2-user/log/ -v=2 &
