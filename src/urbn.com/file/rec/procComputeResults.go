package main

import (
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
	"strings"
	"time"
	"urbn.com/s3"
	"urbn.com/aws-s3"
)

func main() {
	bucket:=flag.String("bucket", "", "")
	flag.Parse()
	svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	history := make(map[string]time.Time)
	init:=true
	dsvc := dynamodb.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	for true {
		time.Sleep(5 * time.Second)
		ok, key := isDataReady(svc, *bucket, history, init)
		init=false
		if ok {
			glog.V(1).Infof("bucket[%s] is ready with key[%s]\n",*bucket,key)
			go processResults(svc, dsvc, *bucket,key)
		}
	}
}
func processResults(s3svc *s3.S3, dsvc *dynamodb.DynamoDB, bkt string, key string) {
	glog.V(2).Infof("process bucket[%s]/key[%s]\n",bkt,key)
	path:=key[:strings.LastIndex(key,"_SUCCESS")]

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bkt), // Required

	}
	resp, err := s3svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())
	}


	for _, obj := range resp.Contents {
		glog.V(2).Infof("s3 object: %s. path:%s", *obj.Key, path)
		if strings.HasPrefix(*obj.Key, path+"part-") {
			glog.V(2).Infof("populating with file %s", *obj.Key)
			aws_s3.GetObjectAsString(s3svc,bkt,*obj.Key)
			//populateRelatedProducts(&results, getObject(svc, bucket, *obj.Key))
		}
	}

}

func
func isDataReady(svc *s3.S3, bkt string, history map[string]time.Time, init bool) (bool, string) {

	glog.V(2).Infof("init flag [%v]\n",init)
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bkt), // Required
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())
	}

	for _, obj := range resp.Contents {
		glog.V(3).Infof("s3 object: %s. \n", *obj.Key)
		if strings.HasSuffix(*obj.Key, "_SUCCESS") {
			v, ok := history[bkt+"/"+*obj.Key]
			if !ok {
				history[bkt+"/"+*obj.Key] = *obj.LastModified
				if !init {
					return true,*obj.Key
				}

			} else {
				if v.Before(*obj.LastModified) {
					history[bkt+"/"+*obj.Key] = *obj.LastModified
					return true, *obj.Key
				}
			}
		}

	}

	return false,""
}
