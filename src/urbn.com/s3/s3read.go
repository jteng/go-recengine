package s3
import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/golang/glog"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"io"
	"strings"
	"bytes"
)

func main(){
	svc := s3.New(session.New(),&aws.Config{Region: aws.String("us-east-1")})
	//ListBuckets(svc)
	ListObjects(svc)
}

func ListBuckets(svc *s3.S3) {
	//svc := s3.New(session.New(),&aws.Config{Region: aws.String("us-east-1")})

	var params *s3.ListBucketsInput
	resp, err := svc.ListBuckets(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Pretty-print the response data.
	fmt.Println(resp)
}

func GetObject(svc *s3.S3,key string)string{
	//svc := s3.New(session.New(),&aws.Config{Region: aws.String("us-east-1")})


	params := &s3.GetObjectInput{
		Bucket:                     aws.String("ecomm-order-items"), // Required
		Key:                        aws.String(key),  // Required
	}
	resp, err := svc.GetObject(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.Fatal(err.Error())
	}

	// Pretty-print the response data.
	fmt.Print(resp)
	size:=int(*resp.ContentLength)
	fmt.Println("size is ",size)
	buffer:=make([]byte,size)
	defer resp.Body.Close()
	var bbuffer bytes.Buffer
	for true {


		num,rerr:=resp.Body.Read(buffer)
		if num>0{
			//fmt.Println("times ",count)
			bbuffer.Write(buffer[:num])
			//bbuffer.WriteString(string(buffer[:num]))
		}else if rerr==io.EOF || rerr!=nil{
			break
		}
	}
	return bbuffer.String()
}

func ListObjects(svc *s3.S3) {

	//svc := s3.New(session.New(),&aws.Config{Region: aws.String("us-east-1")})

	params := &s3.ListObjectsInput{
		Bucket:       aws.String("ecomm-order-items"), // Required
		//Delimiter:    aws.String("Delimiter"),
		//EncodingType: aws.String("EncodingType"),
		//Marker:       aws.String("Marker"),
		//MaxKeys:      aws.Int64(1),
		//Prefix:       aws.String("Prefix"),
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.

		fmt.Printf("%s, %s \n",err.(awserr.Error).Code(),err.(awserr.Error).Error())
		return
	}

	// Pretty-print the response data.
	//fmt.Println(resp)
	for _,obj:=range resp.Contents{
		if strings.HasPrefix(*obj.Key,"recommendations/output.txt/part-"){
			fmt.Println(GetObject(svc,*obj.Key))
		}
	}
}