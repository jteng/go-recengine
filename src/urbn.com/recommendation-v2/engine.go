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
	"html/template"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/scanner"
	"urbn.com/catalog"
	"urbn.com/customer"
)

const (
	HTTP_HEADER_CONTENT_TYPE = "Content-Type"
	HTTP_HEADER_VALUE_JSON   = "application/json; charset=UTF-8"
)

type SearchParam struct {
	ProductId   string
	Location    string
	Limit       int
	Sort        string
	Customer    string
	PrettyPrint bool
}
type Product struct {
	ProductID           string              `json:"productId"`
	ParentCat string
	ImageUrl string
	BoughtTogetherItems BoughtTogetherItems `json:"boughtTogetherItems"`
	AlsoViewedItems     AlsoViewedItems     `json:"alsoViewedItems"`
}

func (p Product) RemoveSelf() {
	var pos int
	for i, v := range p.AlsoViewedItems {
		if v.ProductID == p.ProductID {
			pos = i
			break
		}
	}
	p.AlsoViewedItems = append(p.AlsoViewedItems[:pos], p.AlsoViewedItems[pos+1:]...)
}

type AlsoViewedItem struct {
	ProductID     string       `json:"productId"`
	ProductName   string       `json:"productName"`
	ImageUrl      string       `json:"imageUrl"`
	TotalScore    int          `json:"totalScore"`
	ScoreByRegion RegionScores `json:"scoreByRegion"`
}
type AlsoViewedItems []*AlsoViewedItem

func (p AlsoViewedItems) TopN(n int64) []*AlsoViewedItem {
	sort.Sort(p)
	t := math.Min(float64(n), float64(len(p)))
	return p[:(int64(t))]

}
func (p AlsoViewedItems) Len() int {
	return len(p)
}
func (p AlsoViewedItems) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p AlsoViewedItems) Less(i, j int) bool {
	si := p[i].TotalScore
	sj := p[j].TotalScore
	return si > sj
}

type BoughtTogetherItem struct {
	ProductID     string       `json:"productId"`
	ProductName   string       `json:"productName"`
	ImageUrl      string       `json:"imageUrl"`
	TotalScore    int          `json:"totalScore"`
	ScoreByRegion RegionScores `json:"scoreByRegion"`
}

type BoughtTogetherItems []*BoughtTogetherItem

func (p BoughtTogetherItems) TopN(n int64) []*BoughtTogetherItem {
	sort.Sort(p)
	t := math.Min(float64(n), float64(len(p)))
	return p[:(int64(t))]
}
func (p BoughtTogetherItems) Len() int {
	return len(p)
}
func (p BoughtTogetherItems) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p BoughtTogetherItems) Less(i, j int) bool {
	si := p[i].TotalScore
	sj := p[j].TotalScore
	return si > sj
}

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
		} else {
			geoItem := SortableItem{ItemId: item.ProductID, Score: item.TotalScore}
			result = append(result, geoItem)
		}

	}
	return result
}

type RelatedProducts struct {
	Relates map[string]Product
}
type TemplateDynHandler struct {
	svc *dynamodb.DynamoDB
	Tpl   *template.Template
}

func (t TemplateDynHandler) ParseTemplate() {
	t.Tpl,_=template.ParseFiles("rectemplate.html")

}
func (dhandler *TemplateDynHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	param := GetSearchParams(r, "")
	productStr := GetItemFromDynamoDb(dhandler.svc, param.ProductId)
	glog.V(5).Infof("retrieved from dynamodb: %s\n", productStr)
	var rp Product
	error := json.Unmarshal([]byte(productStr), &rp)
	if error != nil {
		glog.Errorf("failed to unmarshal json %s %s\n", productStr, error.Error())
	} else {
		rp.RemoveSelf()
		var customer *customer.Customer
		if param.Customer != "" {
			customer = GetCustomerFromDynamoDb(dhandler.svc, param.Customer)
		}

		rp.BoughtTogetherItems = rp.BoughtTogetherItems.TopN(int64(param.Limit))
		rp.AlsoViewedItems = rp.AlsoViewedItems.TopN(int64(param.Limit))
		prodIds := make(map[string]bool)
		for _, v := range rp.AlsoViewedItems {
			prodIds[v.ProductID] = true
			//prodIds = append(prodIds, v.ProductID)
		}
		for _, v := range rp.BoughtTogetherItems {
			prodIds[v.ProductID] = true
		}

		prods := BatchGetProducts(dhandler.svc, prodIds)
		for _, v := range rp.AlsoViewedItems {
			p, ok := prods[v.ProductID]
			if ok {
				glog.V(5).Infof("found [%s]=[%s] \n", p.ProductId, p.ImageUrl)
				v.ProductName = p.ProductName
				if customer!=nil{
					v.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
				}else{
					v.ImageUrl=p.ImageUrl
				}
			}
		}
		for _, v := range rp.BoughtTogetherItems {
			p, ok := prods[v.ProductID]
			if ok {
				glog.V(5).Infof("found [%s]=[%s] \n", p.ProductId, p.ImageUrl)
				v.ProductName = p.ProductName
				if customer!=nil{
					v.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
				}else{
					v.ImageUrl=p.ImageUrl
				}
			}
		}

		p,ok:=prods[rp.ProductID]
		if ok{
			rp.ImageUrl=*p.GetPreferredColor(customer.ColorPrefs)
			rp.ParentCat=p.ParentCat
		}
		t,_:=template.ParseFiles("rectemplate.html")
		/*
		t := template.New("fieldname example")
		t, _ = t.Parse(`Product {{.ProductID}}<br><table><tr>
			{{range .BoughtTogetherItems}}
                	<td> {{.ProductID}}<br>{{.ProductName}}<br>{{.ImageUrl}} </td><td> </td>
            		{{end}}
            		</tr></table>`)
            		*/
		//data:=Product{ProductID:"12345",}
		t.Execute(w, rp)
		//dhandler.Tpl.Execute(w,rp)
		//
		glog.V(5).Infof("served %s %+v", r.URL.Path, rp)
		glog.V(2).Infof("served %s", r.URL.Path)
	}

}

type DynamoDbHandler struct {
	svc *dynamodb.DynamoDB
}

func (dhandler *DynamoDbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	param := GetSearchParams(r, "")
	productStr := GetItemFromDynamoDb(dhandler.svc, param.ProductId)
	glog.V(5).Infof("retrieved from dynamodb: %s\n", productStr)
	var rp Product
	error := json.Unmarshal([]byte(productStr), &rp)
	if error != nil {
		glog.Errorf("failed to unmarshal json %s %s\n", productStr, error.Error())
	} else {
		rp.RemoveSelf()
		var customer *customer.Customer
		if param.Customer != "" {
			customer = GetCustomerFromDynamoDb(dhandler.svc, param.Customer)
		}

		rp.BoughtTogetherItems = rp.BoughtTogetherItems.TopN(int64(param.Limit))
		rp.AlsoViewedItems = rp.AlsoViewedItems.TopN(int64(param.Limit))
		prodIds := make(map[string]bool)
		for _, v := range rp.AlsoViewedItems {
			prodIds[v.ProductID] = true
			//prodIds = append(prodIds, v.ProductID)
		}
		for _, v := range rp.BoughtTogetherItems {
			prodIds[v.ProductID] = true
		}
		prodIds[rp.ProductID]=true
		prods := BatchGetProducts(dhandler.svc, prodIds)
		for _, v := range rp.AlsoViewedItems {
			p, ok := prods[v.ProductID]
			if ok {
				glog.V(5).Infof("found [%s]=[%s] \n", p.ProductId, p.ImageUrl)
				v.ProductName = p.ProductName
				//v.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
				if customer!=nil{
					v.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
				}else{
					v.ImageUrl=p.ImageUrl
				}
			}
		}
		for _, v := range rp.BoughtTogetherItems {
			p, ok := prods[v.ProductID]
			if ok {
				glog.V(5).Infof("found [%s]=[%s] \n", p.ProductId, p.ImageUrl)
				v.ProductName = p.ProductName
				if customer!=nil{
					v.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
				}else{
					v.ImageUrl=p.ImageUrl
				}
				//v.ImageUrl = *p.GetPreferredColor(customer.BrandPrefs)
			}
		}

		if param.PrettyPrint {
			jp, _ := json.MarshalIndent(rp, "", "    ")
			w.Header().Set(HTTP_HEADER_CONTENT_TYPE, HTTP_HEADER_VALUE_JSON)
			w.Write([]byte(jp))
		} else {
			json.NewEncoder(w).Encode(rp)
		}
		//
		glog.V(5).Infof("served %s %+v", r.URL.Path, rp)
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

		result.Customer = r.URL.Query().Get("customer")
		result.Sort = r.URL.Query().Get("sort")

		pp := r.URL.Query().Get("pretty")

		if pp != "" && strings.ToLower(pp) == "true" {
			result.PrettyPrint = true
		}

		pathParams := getUrlPathValues(r.URL.Path)

		result.ProductId = pathParams[strings.ToLower("productId")]
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

func populateRelatedProductsParallel(strContent string) RelatedProducts {
	var result = RelatedProducts{Relates: make(map[string]Product)}
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
			result.Relates[rp.ProductID] = rp
		}
	}
	return result
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
	glog.V(2).Infof("productId is %s\n", productId)
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{ // Required
			"productId": { // Required
				S: aws.String(productId),
			},
			// More values...
		},
		TableName: aws.String("ProductRecommendation"), // Required
		AttributesToGet: []*string{
			aws.String("raw"), // Required
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
	glog.V(5).Infof("resp: %v\n", resp)
	glog.V(5).Infof("raw:%s\n", *(resp.Item["raw"].S))
	return *(resp.Item["raw"].S)
}

func GetCustomerFromDynamoDb(svc *dynamodb.DynamoDB, customerId string) *customer.Customer {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{ // Required
			"customerId": { // Required
				S: aws.String(customerId),
			},
			// More values...
		},
		TableName: aws.String("Customer"), // Required
		AttributesToGet: []*string{
			aws.String("BrandPrefs"),
			aws.String("ColorPrefs"),
			aws.String("SizePrefs"),
			// More values...
		},
		ConsistentRead: aws.Bool(true),
	}
	resp, err := svc.GetItem(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.Errorln(err.Error())
		//return "failed to query DynamoDb"
	}

	result := &customer.Customer{}
	if len(resp.Item) != 0 {
		result.CustomerId = customerId
		result.ColorPrefs = resp.Item["ColorPrefs"].SS
		glog.V(4).Infof("found customer[%s] \n", customerId)
	}

	return result
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
			//BoughtTogetherItems: []BoughtTogetherItem{},
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
			//BoughtTogetherItems: []BoughtTogetherItem{},
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
	templateHandler := &TemplateDynHandler{svc: svc}
	templateHandler.ParseTemplate()
	mux := http.NewServeMux()
	mux.Handle("/recommendation/", handler)
	mux.Handle("/recweb/", templateHandler)
	glog.Infof("data source is pointing to dynamo db. servic ready on port 8080")
	glog.Fatal(http.ListenAndServe(":8080", mux))
}

func serveFromS3(s3Location string) {
	var relatedProducts RelatedProducts = RelatedProducts{Relates: make(map[string]Product)}

	if strings.HasPrefix(s3Location, "s3://") {
		svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
		//relatedProducts = GetRelatedProductsFromS3(svc, s3Location)
		parallelLoadS3(&relatedProducts, svc, s3Location)
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

func nextUrlPathValue(s scanner.Scanner) string {
	var result string
	for s.Scan() != scanner.EOF {
		v := s.TokenText()
		if v != "/" {
			result = v
			break
		}
	}
	return result
}

func getUrlPathValues(path string) map[string]string {
	glog.V(4).Infof("path is %s \n", path)
	var result map[string]string = make(map[string]string)
	var s scanner.Scanner
	s.Init(strings.NewReader(path))
	var tok rune
	for tok != scanner.EOF {
		tok = s.Scan()
		v := s.TokenText()
		if v != "/" {
			fieldName := v

			value := nextUrlPathValue(s)
			result[strings.ToLower(fieldName)] = value
		}
	}
	for k, v := range result {
		glog.V(4).Infof("key:[%s]||value:[%s]\n", k, v)
	}
	return result
}

func parallelLoadS3(relates *RelatedProducts, svc *s3.S3, dataDir string) {
	bucket, keypattern := parseS3Params(dataDir)
	params := &s3.ListObjectsInput{
		Bucket: aws.String("ecomm-order-items"), // Required
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())
	}

	workQueue := make(chan Payload, 25)
	statusQueue := make(chan RelatedProducts, 25)

	processWorkQueue(workQueue, statusQueue)
	var mutex = &sync.Mutex{}
	num := 0
	for _, obj := range resp.Contents {
		glog.V(2).Infof("s3 object: %s. Keypattern:%s", *obj.Key, keypattern)
		if strings.HasPrefix(*obj.Key, keypattern[1:]+"/part-") {
			glog.V(2).Infof("populating with file %s", *obj.Key)
			payLoad := Payload{RelatedItems: relates, Bucket: bucket, Key: *obj.Key, SVC: svc, Mutex: mutex}
			workQueue <- payLoad
			num++
			//populateRelatedProducts(relates, getObject(svc, bucket, *obj.Key))
		}
	}
	//close(workQueue)

	for num > 0 {
		r := <-statusQueue
		for k, v := range r.Relates {
			relates.Relates[k] = v
		}
		num--
	}
	glog.V(1).Infof("load all files \n")

}

func processWorkQueue(q chan Payload, s chan RelatedProducts) {
	for i := 0; i < 16; i++ {
		go func() {
			for payload := range q {
				result := populateRelatedProductsParallel(getObject(payload.SVC, payload.Bucket, payload.Key))
				glog.V(3).Infof("file %s loaded \n", payload.Key)
				s <- result
			}

		}()
	}
}

type Payload struct {
	RelatedItems *RelatedProducts
	SVC          *s3.S3
	Bucket       string
	Key          string
	Mutex        *sync.Mutex
}

func BatchGetProducts(svc *dynamodb.DynamoDB, prodIds map[string]bool) map[string]*catalog.Product {
	result := make(map[string]*catalog.Product)
	//svc := dynamodb.New(session.New())
	var keys []map[string]*dynamodb.AttributeValue
	for v, _ := range prodIds {
		k := make(map[string]*dynamodb.AttributeValue)
		k["productId"] = &dynamodb.AttributeValue{
			S: aws.String(v),
		}
		keys = append(keys, k)
	}
	params := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{ // Required
			"UoProducts": { // Required
				Keys: keys,
				AttributesToGet: []*string{
					aws.String("ProductName"),
					aws.String("LeadColorImageUrl"),
					aws.String("productId"),
					aws.String("ImageUrls"),
					aws.String("ParentCategory"),
					// More values...
				},
				ConsistentRead: aws.Bool(true),
			},
			// More values...
		},
	}
	resp, err := svc.BatchGetItem(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.V(1).Infoln(err.Error())

	}

	for _, i := range resp.Responses["UoProducts"] {
		p := &catalog.Product{
			ImageUrls: make(map[string]*string),
		}
		for a, v := range i {
			switch a {
			case "productId":
				p.ProductId = *v.S
			case "ProductName":
				p.ProductName = *v.S
			case "LeadColorImageUrl":
				p.ImageUrl = *v.S
				glog.V(5).Infof("set lead color image url for %s to %s \n", p.ProductId, p.ImageUrl)
			case "ImageUrls":
				for c, v := range v.M {
					p.ImageUrls[c] = v.S
				}
			case "ParentCategory":
				p.ParentCat=*v.S
			}
		}
		result[p.ProductId] = p
	}
	return result
}
