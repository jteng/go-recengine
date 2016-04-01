#!/bin/bash
aws s3 rm s3://ecomm-order-items/boughtTogether/ --recursive
spark-submit --class "ComputeBoughtTogetherApp" --master yarn ./urbn-rec-bd-1.2.jar s3input=s3://ecomm-order-items/flatByOrderid.txt s3output=s3://ecomm-order-items/boughtTogether
