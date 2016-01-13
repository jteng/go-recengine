package main

import (
	"flag"
	"net/http"
	"strings"
	"encoding/json"
	"io/ioutil"
	"github.com/golang/glog"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"bytes"
	"io"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
)

const (
	HTTP_HEADER_CONTENT_TYPE = "Content-Type"
	HTTP_HEADER_VALUE_JSON   = "application/json; charset=UTF-8"
)

type Product struct{
	ProductID     string `json:"productId"`
	SortedRelates []RelatedProduct `json:"sortedRelates"`
}
type RelatedProduct struct {
	ProductID     string `json:"productId"`
	Score         int    `json:"score"`
}

type RelatedProducts struct{
	Relates map[string]Product
}

func (relates *RelatedProducts) ServeHTTP(w http.ResponseWriter, r *http.Request){
	glog.V(2).Infof("serving %s",r.URL.Path)
	productId:=GetProductId(r)
	var prod Product
	if val,ok:=relates.Relates[productId]; ok{
		prod=val
	}else{
		//var recom [] RelatedProduct=[2]RelatedProduct{RelatedProduct{ProductID:"abcd",Score:25},RelatedProduct{ProductID:"xdgg",Score:2}}
		prod=Product{
			ProductID:productId,
			SortedRelates:[]RelatedProduct{},
		}
	}

	w.Header().Set(HTTP_HEADER_CONTENT_TYPE, HTTP_HEADER_VALUE_JSON)
	json.NewEncoder(w).Encode(prod)
}

//get the productId from the url path, for instance /recommendation/prod123 will return prod123
func GetProductId(r *http.Request) string{
	p := strings.Split(r.URL.Path, "/")
	length:=len(p)
	if length>=1{
		return p[length-1]
	}else {
		return ""
	}
}
//get the RelatedProducts from the data directory. the data file name following the pattern of "part([\d]+)"

func GetRelatedProducts(dataDir string) RelatedProducts{
	results:=RelatedProducts{Relates:make(map[string]Product)}
	fileInfos,error:=ioutil.ReadDir(dataDir)
	if error!=nil {
		glog.Fatalf("failed to read the data directory %s \n",dataDir)
	}
	for _,fileInfo:=range fileInfos{
		if strings.Contains(fileInfo.Name(),".crc"){
			continue
		}else if strings.Contains(fileInfo.Name(),"part-"){
			contents,error:=ioutil.ReadFile(dataDir+"/"+fileInfo.Name())
			if error!=nil{
				glog.Fatalf("failed to load data file %s %s\n",dataDir+"/"+fileInfo.Name(),error.Error())
			}
			strContent:=string(contents)
			parts:=strings.Split(strContent,"\n")
			for _,part:=range parts{
				//get the json which starts from the first { and ends at the last )
				start:=strings.Index(part,"{")
				end:=strings.LastIndex(part,")")
				if start<0 || end>=len(part){
					continue
				}
				jsonStr:=part[start:end]
				var rp Product
				error:=json.Unmarshal([]byte(jsonStr),&rp)
				if error!=nil{
					glog.Errorf("failed to unmarshal json %s %s\n",jsonStr,error.Error())
				}else{
					results.Relates[rp.ProductID]=rp
				}
			}
		}
	}
	return results
}

func GetRelatedProductsFromS3(svc *s3.S3, dataDir string) RelatedProducts{
	results:=RelatedProducts{Relates:make(map[string]Product)}
	bucket,keypattern:=parseS3Params(dataDir)
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

		glog.Errorf("%s, %s \n",err.(awserr.Error).Code(),err.(awserr.Error).Error())

	}

	// Pretty-print the response data.
	//fmt.Println(resp)
	for _,obj:=range resp.Contents{
		glog.V(2).Infof("s3 object: %s. Keypattern:%s",*obj.Key,keypattern)
		if strings.HasPrefix(*obj.Key,keypattern[1:]+"/part-"){
			glog.V(2).Infof("populating with file %s",*obj.Key)
			populateRelatedProducts(&results,getObject(svc, bucket, *obj.Key))
		}
	}
	glog.V(2).Infof("35671072 is %s \n",results.Relates["35671072"])
	return results

}
//populate relatedProducts by parsing out the strContent which should the content from part-00000[\d] files
func populateRelatedProducts(relatedProducts *RelatedProducts,strContent string){
	parts:=strings.Split(strContent,"\n")
	glog.V(2).Infof("number of results: %d",len(parts))
	for _,part:=range parts{
		//get the json which starts from the first { and ends at the last )
		start:=strings.Index(part,"{")
		end:=strings.LastIndex(part,")")
		if start<0 || end>=len(part){
			continue
		}
		jsonStr:=part[start:end]

		var rp Product
		error:=json.Unmarshal([]byte(jsonStr),&rp)
		if error!=nil{
			glog.Errorf("failed to unmarshal json %s %s\n",jsonStr,error.Error())
		}else{
			relatedProducts.Relates[rp.ProductID]=rp
		}
	}
}
//get object from s3
func getObject(svc *s3.S3,bucket string, key string)string{
	params := &s3.GetObjectInput{
		Bucket:                     aws.String(bucket), // Required
		Key:                        aws.String(key),  // Required
	}
	resp, err := svc.GetObject(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.Fatal(err.Error())
	}

	size:=int(*resp.ContentLength)

	buffer:=make([]byte,size)
	defer resp.Body.Close()
	var bbuffer bytes.Buffer
	for true {
		num,rerr:=resp.Body.Read(buffer)
		if num>0{
			bbuffer.Write(buffer[:num])
		}else if rerr==io.EOF || rerr!=nil{
			break
		}
	}
	return bbuffer.String()
}
//parse s3://ecomm-order-items/recommendations/output.txt to return {ecomm-order-items,recommendations/output.txt}
func parseS3Params(in string)(string,string){
	if strings.HasPrefix(in,"s3://"){
		params:=in[len("s3://"):]
		parts:=strings.Split(params,"/")
		return parts[0],params[len(parts[0]):]
	}
	return "",""
}

func main(){
	dataDir:=flag.String("dataLocation","","")
	flag.Parse()
	glog.V(2).Infof("data dir is %s \n",*dataDir)

	var relatedProducts RelatedProducts

	if strings.HasPrefix(*dataDir,"s3://"){
		svc := s3.New(session.New(),&aws.Config{Region: aws.String("us-east-1")})
		relatedProducts=GetRelatedProductsFromS3(svc,*dataDir)
	}else {
		relatedProducts=GetRelatedProducts(*dataDir)
	}

	mux := http.NewServeMux()

	myHandler := &relatedProducts
	mux.Handle("/recommendation/", myHandler)
	glog.Infof("servic ready on port 80")
	glog.Fatal(http.ListenAndServe(":80", mux))

}