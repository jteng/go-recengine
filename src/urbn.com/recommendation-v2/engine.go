package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
"text/scanner"
)

const (
	HTTP_HEADER_CONTENT_TYPE = "Content-Type"
	HTTP_HEADER_VALUE_JSON   = "application/json; charset=UTF-8"
)

type SearchParam struct {
	ProductId string
	Location  string
	Limit     int
}
type Product struct {
	ProductID           string              `json:"productId"`
	BoughtTogetherItems BoughtTogetherItems `json:"boughtTogetherItems"`
}

type BoughtTogetherItem struct {
	ProductID     string       `json:"productId"`
	TotalScore    int          `json:"totalScore"`
	ScoreByRegion RegionScores `json:"scoreByRegion"`
}

type BoughtTogetherItems []BoughtTogetherItem

type RegionScore struct {
	Region string `json:"region"`
	Score  int    `json:"score"`
}

type RegionScores []RegionScore

//define the sortable item GeoItem and its sort implementation
type SortableItem struct {
	//Target string
	ItemId string
	Score  int
	//Item   BoughtTogetherItem
}
type SortableItems []SortableItem

func (slice SortableItems) Len() int {
	return len(slice)
}
func (slice SortableItems) Less(i, j int) bool {
	//find the RegionScore for the target region first
	iItem := slice[i]
	jItem := slice[j]
	return iItem.Score > jItem.Score
}
func (slice SortableItems) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func convertToSortableItems(items BoughtTogetherItems, searchParam SearchParam) SortableItems {
	var result SortableItems
	for _, item := range items {
		if item.ProductID == searchParam.ProductId {
			continue
		}
		if searchParam.Location != "" {
			for _, score := range item.ScoreByRegion {
				if score.Region == strings.ToUpper(searchParam.Location) {
					geoItem := SortableItem{ItemId: item.ProductID, Score: score.Score}
					result = append(result, geoItem)
					break
				}
			}
		}else{
			geoItem := SortableItem{ItemId: item.ProductID, Score: item.TotalScore}
			result = append(result, geoItem)
		}

	}
	return result
}

type RelatedProducts struct {
	Relates map[string]Product
}
type DynamoDbHandler struct {
	svc *dynamodb.DynamoDB
}

func (dhandler *DynamoDbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	param := GetSearchParams(r, "")
	productStr := GetItemFromDynamoDb(dhandler.svc, param.ProductId)
	var rp Product
	error := json.Unmarshal([]byte(productStr), &rp)
	if error != nil {
		glog.Errorf("failed to unmarshal json %s %s\n", productStr, error.Error())
	} else {
		//recommendation:=convertToRecommendation(rp)
		json.NewEncoder(w).Encode(rp)
		glog.V(3).Infof("served %s %+v", r.URL.Path, rp)
		glog.V(2).Infof("served %s", r.URL.Path)
	}

}

func filterResult(prod Product, limit int) Product {
	if len(prod.BoughtTogetherItems) > 0 {
		min := math.Min(float64(limit), (float64(len(prod.BoughtTogetherItems))))
		filtered := prod.BoughtTogetherItems[0:int(min)]
		prod.BoughtTogetherItems = filtered
	}
	return prod
}

func filterRecommendation(items SortableItems, limit int) SortableItems {
	if len(items) > 0 {
		min := math.Min(float64(limit), (float64(len(items))))
		return items[0:int(min)]

	}
	return items
}

//get the productId from the url path, for instance /recommendation/prod123 will return prod123
// /recommendation/prod123/us_mid-west will return prod123,us_mid-west
func GetSearchParams(r *http.Request, pathBound string) SearchParam {
	result := SearchParam{Limit: 10}
	if strings.HasPrefix(r.URL.Path, pathBound) {
		//params := r.URL.Path[len(pathBound):]
		limitStr := r.URL.Query().Get("limit")
		if limitStr != "" {
			limit, error := strconv.ParseInt(limitStr, 10, 64)
			if error == nil {
				result.Limit = int(limit)
			}
		}

		location := r.URL.Query().Get("location")
		if location != "" {
			result.Location = strings.ToUpper(location)
		}

		pathParams:=getUrlPathValues(r.URL.Path);

		result.ProductId=pathParams[strings.ToLower("productId")]
		glog.V(3).Infof("search params %v ", result)

	}

	glog.V(3).Infof("search param for path %v is %v \n", r.URL.Path, result)
	return result
}

//get the RelatedProducts from the data directory. the data file name following the pattern of "part([\d]+)"

func GetRelatedProducts(dataDir string) RelatedProducts {
	results := RelatedProducts{Relates: make(map[string]Product)}
	fileInfos, error := ioutil.ReadDir(dataDir)
	if error != nil {
		glog.Fatalf("failed to read the data directory %s \n", dataDir)
	}
	for _, fileInfo := range fileInfos {
		if strings.Contains(fileInfo.Name(), ".crc") {
			continue
		} else if strings.Contains(fileInfo.Name(), "part-") {
			contents, error := ioutil.ReadFile(dataDir + "/" + fileInfo.Name())
			if error != nil {
				glog.Fatalf("failed to load data file %s %s\n", dataDir+"/"+fileInfo.Name(), error.Error())
			}
			strContent := string(contents)
			parts := strings.Split(strContent, "\n")
			for _, part := range parts {
				//get the json which starts from the first { and ends at the last )
				start := strings.Index(part, "{")
				end := strings.LastIndex(part, ")")
				if start < 0 || end >= len(part) {
					continue
				}
				jsonStr := part[start:end]
				var rp Product
				error := json.Unmarshal([]byte(jsonStr), &rp)
				if error != nil {
					glog.Errorf("failed to unmarshal json %s %s\n", jsonStr, error.Error())
				} else {
					results.Relates[rp.ProductID] = rp
				}
			}
		}
	}
	return results
}

func GetRelatedProductsFromS3(svc *s3.S3, dataDir string) RelatedProducts {
	results := RelatedProducts{Relates: make(map[string]Product)}
	bucket, keypattern := parseS3Params(dataDir)
	params := &s3.ListObjectsInput{
		Bucket: aws.String("ecomm-order-items"), // Required
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

		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())

	}

	// Pretty-print the response data.
	//fmt.Println(resp)
	for _, obj := range resp.Contents {
		glog.V(2).Infof("s3 object: %s. Keypattern:%s", *obj.Key, keypattern)
		if strings.HasPrefix(*obj.Key, keypattern[1:]+"/part-") {
			glog.V(2).Infof("populating with file %s", *obj.Key)
			populateRelatedProducts(&results, getObject(svc, bucket, *obj.Key))
		}
	}

	return results

}

//populate relatedProducts by parsing out the strContent which should the content from part-00000[\d] files
func populateRelatedProducts(relatedProducts *RelatedProducts, strContent string) {
	parts := strings.Split(strContent, "\n")
	glog.V(2).Infof("number of results: %d", len(parts))
	for _, part := range parts {
		//get the json which starts from the first { and ends at the last )
		start := strings.Index(part, "{")
		end := strings.LastIndex(part, ")")
		if start < 0 || end >= len(part) {
			continue
		}
		jsonStr := part[start:end]

		var rp Product
		error := json.Unmarshal([]byte(jsonStr), &rp)
		if error != nil {
			glog.Errorf("failed to unmarshal json %s %s\n", jsonStr, error.Error())
		} else {
			relatedProducts.Relates[rp.ProductID] = rp
		}
	}
}

//get object from s3
func getObject(svc *s3.S3, bucket string, key string) string {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket), // Required
		Key:    aws.String(key),    // Required
	}
	resp, err := svc.GetObject(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.Fatal(err.Error())
	}

	size := int(*resp.ContentLength)

	buffer := make([]byte, size)
	defer resp.Body.Close()
	var bbuffer bytes.Buffer
	for true {
		num, rerr := resp.Body.Read(buffer)
		if num > 0 {
			bbuffer.Write(buffer[:num])
		} else if rerr == io.EOF || rerr != nil {
			break
		}
	}
	return bbuffer.String()
}

//parse s3://ecomm-order-items/recommendations/output.txt to return {ecomm-order-items,recommendations/output.txt}
func parseS3Params(in string) (string, string) {
	if strings.HasPrefix(in, "s3://") {
		params := in[len("s3://"):]
		parts := strings.Split(params, "/")
		return parts[0], params[len(parts[0]):]
	}
	return "", ""
}

func GetItemFromDynamoDb(svc *dynamodb.DynamoDB, productId string) string {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{ // Required
			"productId": { // Required
				S: aws.String(productId),
			},
			// More values...
		},
		TableName: aws.String("ProductRecommendation"), // Required
		AttributesToGet: []*string{
			aws.String("boughtTogether"), // Required
			// More values...
		},
		ConsistentRead: aws.Bool(true),
		/*
			ExpressionAttributeNames: map[string]*string{
				"Key": aws.String("AttributeName"), // Required
				// More values...
			},
			ProjectionExpression:   aws.String("ProjectionExpression"),
			ReturnConsumedCapacity: aws.String("ReturnConsumedCapacity"),
		*/
	}
	resp, err := svc.GetItem(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.Errorln(err.Error())
		return "failed to query DynamoDb"
	}

	// Pretty-print the response data.
	return *resp.Item["boughtTogether"].S
}

type RawHandler struct {
	pathBound string
	data      *RelatedProducts
}

func (relates *RawHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	glog.V(2).Infof("serving %s", r.URL.Path)
	param := GetSearchParams(r, relates.pathBound)
	var prod Product
	relatesProducts := *relates.data
	if val, ok := relatesProducts.Relates[param.ProductId]; ok {
		prod = val
	} else {
		prod = Product{
			ProductID: param.ProductId,
			//SortedRelates:[]RelatedProduct{},
			BoughtTogetherItems: []BoughtTogetherItem{},
		}
	}

	w.Header().Set(HTTP_HEADER_CONTENT_TYPE, HTTP_HEADER_VALUE_JSON)
	prod = filterResult(prod, param.Limit)
	//items:=filterRecommendation(prod)
	json.NewEncoder(w).Encode(prod)
	glog.V(4).Infof("served %s %+v", r.URL.Path, prod)
	glog.V(2).Infof("served %s", r.URL.Path)

}

type GeoHandler struct {
	pathBound string
	data      *RelatedProducts
}

func (relates *GeoHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	glog.V(2).Infof("serving %s", r.URL.Path)
	param := GetSearchParams(r, relates.pathBound)
	var prod Product
	relatesProducts := *relates.data
	if val, ok := relatesProducts.Relates[param.ProductId]; ok {
		prod = val
	} else {
		prod = Product{
			ProductID: param.ProductId,
			//SortedRelates:[]RelatedProduct{},
			BoughtTogetherItems: []BoughtTogetherItem{},
		}
	}

	recommendation := convertToSortableItems(prod.BoughtTogetherItems, param)

	sort.Sort(recommendation)

	w.Header().Set(HTTP_HEADER_CONTENT_TYPE, HTTP_HEADER_VALUE_JSON)
	//prod = filterResult(prod,param.Limit)
	items := filterRecommendation(recommendation, param.Limit)
	json.NewEncoder(w).Encode(items)
	glog.V(4).Infof("served %s %+v", r.URL.Path, items)
	glog.V(2).Infof("served %s", r.URL.Path)

}

func main() {
	dataDir := flag.String("dataLocation", "", "")
	useDynamoDb := flag.Bool("useDynamoDb", false, "dynamodb indicator")
	flag.Parse()
	glog.V(2).Infof("data dir is %s \n", *dataDir)
	if !*useDynamoDb {
		serveFromS3(*dataDir)
	} else {
		serveFromDynamoDb()
	}
}

func serveFromDynamoDb() {
	svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	handler := &DynamoDbHandler{svc: svc}
	mux := http.NewServeMux()
	mux.Handle("/recommendation/", handler)
	glog.Infof("data source is pointing to dynamo db. servic ready on port 8080")
	glog.Fatal(http.ListenAndServe(":8080", mux))
}

func serveFromS3(s3Location string) {
	var relatedProducts RelatedProducts

	if strings.HasPrefix(s3Location, "s3://") {
		svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
		relatedProducts = GetRelatedProductsFromS3(svc, s3Location)
	} else {
		relatedProducts = GetRelatedProducts(s3Location)
	}

	geoHandler := GeoHandler{data: &relatedProducts, pathBound: "/recommendation/"}
	rawHandler := RawHandler{data: &relatedProducts, pathBound: "/recommendation/raw/"}
	mux := http.NewServeMux()

	mux.Handle("/recommendation/raw/", &rawHandler)
	mux.Handle("/recommendation/", &geoHandler)
	glog.Infof("servic ready on port 8080")
	glog.Fatal(http.ListenAndServe(":8080", mux))
}

func nextUrlPathValue(s scanner.Scanner) string{
	var result string
	for s.Scan()!=scanner.EOF{
		v:=s.TokenText()
		if v!="/"{
			result=v
			break
		}
	}
	return result
}

func getUrlPathValues(path string) map[string]string{
	glog.V(4).Infof("path is %s \n",path)
	var result map[string]string = make(map[string]string)
	var s scanner.Scanner
	s.Init(strings.NewReader(path))
	var tok rune
	for tok!=scanner.EOF{
		tok=s.Scan()
		v:=s.TokenText()
		if v!="/"{
			fieldName:=v

			value:=nextUrlPathValue(s)
			result[strings.ToLower(fieldName)]=value
		}
	}
	for k,v:=range result{
		glog.V(4).Infof("key:[%s]||value:[%s]\n",k,v)
	}
	return result
}