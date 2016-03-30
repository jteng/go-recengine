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
	"urbn.com/recommendation"
)

const (
	HTTP_HEADER_CONTENT_TYPE = "Content-Type"
	HTTP_HEADER_VALUE_JSON   = "application/json; charset=UTF-8"
)

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

func convertToSortableItems(items recommendation.Items, searchParam recommendation.SearchParam) SortableItems {
	var result SortableItems
	for _, item := range items {
		if item.ProductID == searchParam.ProductId {
			continue
		}
		if searchParam.Location != "" {
			for _, score := range item.ScoresByRegion {
				if *score.Region == strings.ToUpper(searchParam.Location) {
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

type TemplateDynHandler struct {
	svc *dynamodb.DynamoDB
	Tpl *template.Template
}

func (t TemplateDynHandler) ParseTemplate() {
	t.Tpl, _ = template.ParseFiles("rectemplate.html")

}
func (dhandler *TemplateDynHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	param := GetSearchParams(r, "")

	rp, err := GetProdRec(dhandler.svc, param.ProductId)

	if err != nil {
		glog.Errorf("failed to get product recommendation [%s] %v\n", param.ProductId, err)
	} else {
		MakeRecommendation(rp, dhandler.svc, &param)
		//parse template each time is NOT a good idea
		//should we cache it?
		t, _ := template.ParseFiles("rectemplate.html")

		t.Execute(w, rp)

		glog.V(5).Infof("served %s boughtTogether[%d]/alsoViewed[%d]\n", r.URL.Path, len(rp.BoughtTogetherItems), len(rp.AlsoViewedItems))
		glog.V(2).Infof("served %s", r.URL.Path)
	}

}

func MakeRecommendation(rp *recommendation.Product, svc *dynamodb.DynamoDB, param *recommendation.SearchParam) {
	rp.RemoveSelf()
	var customer *customer.Customer
	if param.Customer != "" {
		customer = GetCustomerFromDynamoDb(svc, param.Customer)
	}

	rp.BoughtTogetherItems = rp.BoughtTogetherItems.TopN(param.Limit * 2)
	rp.AlsoViewedItems = rp.AlsoViewedItems.TopN(param.Limit * 2)

	prodIds := make(map[string]bool)
	prodIds[rp.ProductID] = true
	for _, v := range rp.AlsoViewedItems {
		prodIds[v.ProductID] = true
	}
	for _, v := range rp.BoughtTogetherItems {
		prodIds[v.ProductID] = true
	}
	//get the product detail info from product catalog
	prods := BatchGetProducts(svc, prodIds)
	p, ok := prods[rp.ProductID]
	//populate the current product
	if ok {
		if customer != nil {
			rp.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
		} else {
			rp.ImageUrl = p.ImageUrl
		}
		rp.ParentCat = p.ParentCat
		rp.SalesByRegion = p.SalesByRegion
		rp.NumOfPurchase = p.NumOfPurchase
		glog.V(2).Infof("found current prod %s %d \n", rp.ProductID, p.NumOfPurchase)
		sIds := make(map[string]bool)
		for _, v := range p.SimilarItems {
			sIds[*v] = true
		}
		//get the detail info for each similar products of the current product
		//may consider have the similar products to ProductRecommendation doc
		simprods := BatchGetProducts(svc, sIds)
		for _, sp := range simprods {
			if sp.Availability {
				si := recommendation.Item{Type: recommendation.REC_ITEM_TYPE_SIMILAR}
				si.ProductID = sp.ProductId
				si.ImageUrl = sp.ImageUrl
				si.TotalScore = sp.NumOfPurchase
				si.ProductName = sp.ProductName
				si.ParentCat = sp.ParentCat
				si.ScoresByRegion = sp.SalesByRegion
				si.Availability = sp.Availability
				si.OnSale=sp.IsOnSale(param.Currency)
				glog.V(4).Infof("Item[%s] is on sale %v %v in %s \n",si.ProductID,si.OnSale,sp.IsOnSale(param.Currency),param.Currency)
				rp.BestSellers = append(rp.BestSellers, &si)
			}
		}
	}

	avm := make(map[string]*recommendation.Item)
	for _, v := range rp.AlsoViewedItems {
		avm[v.ProductID] = v
		p, ok := prods[v.ProductID]
		if ok {
			glog.V(5).Infof("found [%s]=[%s] \n", p.ProductId, p.ImageUrl)
			v.ProductName = p.ProductName
			v.ParentCat = p.ParentCat
			v.Availability = p.Availability
			glog.V(3).Infof("set item[%s].ParentCat[%s]  \n", v.ProductID, v.ParentCat)
			if customer != nil {
				v.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
			} else {
				v.ImageUrl = p.ImageUrl
			}
		}
	}
	btm := make(map[string]*recommendation.Item)
	for _, v := range rp.BoughtTogetherItems {
		btm[v.ProductID] = v
		p, ok := prods[v.ProductID]
		if ok {
			glog.V(5).Infof("found [%s]=[%s] \n", p.ProductId, p.ImageUrl)
			v.ProductName = p.ProductName
			v.ParentCat = p.ParentCat
			v.Availability = p.Availability
			if customer != nil {
				v.ImageUrl = *p.GetPreferredColor(customer.ColorPrefs)
			} else {
				v.ImageUrl = p.ImageUrl
			}
		}

	}

	bsm := make(map[string]*recommendation.Item)
	for pid, v := range avm {
		//item:=recommendation.Item(*v)
		bi, ok := btm[pid]
		glog.V(3).Infof("parent cat for %s is %s. item[%s].parentCat=[%s]\n", rp.ProductID, rp.ParentCat, v.ProductID, v.ParentCat)
		if ok {
			if bi.TotalScore<=v.TotalScore{
				v.TotalScore =v.TotalScore*2
			}else{
				v.TotalScore=bi.TotalScore*2
			}
			v.Type = recommendation.REC_ITEM_TYPE_PICKED
			glog.V(3).Infof("increase weight to %d for product[%s] as it is also in bt list\n",v.TotalScore,v.ProductID)
			//item:=recommendation.Item(*v)
			bsm[v.ProductID] = v
		} else if rp.ParentCat == v.ParentCat {
			//item.Type="A"
			//item:=recommendation.Item(*v)
			bsm[v.ProductID] = v
		} else if v.IsSignificant(){
			bsm[v.ProductID] = v
		}
	}
	for pid, v := range btm {
		//item:=recommendation.Item(*v)
		_, ok := bsm[pid]
		if !ok {
			if rp.ParentCat == v.ParentCat {
				//item.Type=recommendation.REC_ITEM_TYPE_BOUGHTTOGETHER
				bsm[v.ProductID] = v
			}else if v.IsSignificant(){
				bsm[v.ProductID] = v
			}
		}
	}
	rp.BestSellers = rp.BestSellers.TopN(param)

	for _, i := range bsm {
		if i.Availability{
			rp.PickedForU = append(rp.PickedForU, i)
		}
	}
	if len(rp.PickedForU)<param.Limit{
		for i:=0;len(rp.PickedForU)<param.Limit&&i<len(rp.BestSellers);i++{
			rp.PickedForU=append(rp.PickedForU,rp.BestSellers[i])
		}
	}
	//sort.Sort(rp.BestSimilar)
	rp.BoughtTogetherItems = rp.BoughtTogetherItems.TopN(param)
	rp.BoughtTogetherItems.RemoveDups()
	rp.AlsoViewedItems = rp.AlsoViewedItems.TopN(param)
	rp.AlsoViewedItems.RemoveDups()
	rp.PickedForU = rp.PickedForU.TopN(param)

	//rp.SalesByRegion=rp.SalesByRegion.TopN(param.Limit)

}

type DynamoDbHandler struct {
	svc *dynamodb.DynamoDB
}

func (dhandler *DynamoDbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	param := GetSearchParams(r, "")

	rp, err := GetProdRec(dhandler.svc, param.ProductId)

	if err != nil {
		glog.Errorf("failed to get product recommendation [%s] %v\n", param.ProductId, err)
	} else {
		MakeRecommendation(rp, dhandler.svc, &param)
		if param.PrettyPrint {
			jp, _ := json.MarshalIndent(rp, "", "    ")
			w.Header().Set(HTTP_HEADER_CONTENT_TYPE, HTTP_HEADER_VALUE_JSON)
			w.Write([]byte(jp))
		} else {
			json.NewEncoder(w).Encode(rp)
		}
		//
		glog.V(5).Infof("served %s boughtTogether[%d]/alsoViewed[%d]\n", r.URL.Path, len(rp.BoughtTogetherItems), len(rp.AlsoViewedItems))
		glog.V(2).Infof("served %s", r.URL.Path)
	}

}

func filterResult(prod recommendation.Product, limit int) recommendation.Product {
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
func GetSearchParams(r *http.Request, pathBound string) recommendation.SearchParam {
	result := recommendation.SearchParam{Limit: 10}
	if strings.HasPrefix(r.URL.Path, pathBound) {
		//params := r.URL.Path[len(pathBound):]
		limitStr := r.URL.Query().Get("limit")
		if limitStr != "" {
			limit, error := strconv.ParseInt(limitStr, 0, 64)
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
		result.Currency=r.URL.Query().Get("currency")
		if result.Currency==""{
			result.Currency=catalog.CURRENCY_CODE_USD
		}

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

func GetRelatedProducts(dataDir string) recommendation.RelatedProducts {
	results := recommendation.RelatedProducts{Relates: make(map[string]*recommendation.Product)}
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
				var rp recommendation.Product
				error := json.Unmarshal([]byte(jsonStr), &rp)
				if error != nil {
					glog.Errorf("failed to unmarshal json %s %s\n", jsonStr, error.Error())
				} else {
					results.Relates[rp.ProductID] = &rp
				}
			}
		}
	}
	return results
}

func GetRelatedProductsFromS3(svc *s3.S3, dataDir string) recommendation.RelatedProducts {
	results := recommendation.RelatedProducts{Relates: make(map[string]*recommendation.Product)}
	bucket, keypattern := parseS3Params(dataDir)
	params := &s3.ListObjectsInput{
		Bucket: aws.String("ecomm-order-items"), // Required
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())

	}

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
func populateRelatedProducts(relatedProducts *recommendation.RelatedProducts, strContent string) {
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

		var rp recommendation.Product
		error := json.Unmarshal([]byte(jsonStr), &rp)
		if error != nil {
			glog.Errorf("failed to unmarshal json %s %s\n", jsonStr, error.Error())
		} else {
			relatedProducts.Relates[rp.ProductID] = &rp
		}
	}

}

func populateRelatedProductsParallel(strContent string) recommendation.RelatedProducts {
	var result = recommendation.RelatedProducts{Relates: make(map[string]*recommendation.Product)}
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

		var rp recommendation.Product
		error := json.Unmarshal([]byte(jsonStr), &rp)
		if error != nil {
			glog.Errorf("failed to unmarshal json %s %s\n", jsonStr, error.Error())
		} else {
			result.Relates[rp.ProductID] = &rp
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

func GetProdRec(svc *dynamodb.DynamoDB, productId string) (*recommendation.Product, error) {
	rp := &recommendation.Product{ProductID: productId}
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
			aws.String("avItems"),
			aws.String("btItems"),
			// More values...
		},
		ConsistentRead: aws.Bool(true),
	}
	resp, err := svc.GetItem(params)

	if err != nil {
		glog.Errorln(err.Error())
		return nil, err
	}
	avItems, ok := resp.Item["avItems"]
	if ok {
		for _, i := range avItems.M {
			avi := recommendation.Item{
				Type:       recommendation.REC_ITEM_TYPE_ALSOVIEWED,
				ProductID:  *i.M["ProductId"].S,
				TotalScore: convertScore(*i.M["TotalScore"].N),
				//ScoresByRegion:convertToScoreByRegion(i.M["ScoreByRegion"].L),
			}
			v, ok := i.M["ScoreByRegion"]
			if ok {
				avi.ScoresByRegion = convertToScoreByRegion(v.L)
			}
			rp.AlsoViewedItems = append(rp.AlsoViewedItems, &avi)
		}
	}
	btItems, ok := resp.Item["btItems"]
	if ok {
		for _, i := range btItems.M {
			bt := recommendation.Item{
				Type:           recommendation.REC_ITEM_TYPE_BOUGHTTOGETHER,
				ProductID:      *i.M["ProductId"].S,
				TotalScore:     convertScore(*i.M["TotalScore"].N),
				ScoresByRegion: convertToScoreByRegion(i.M["ScoreByRegion"].L),
			}
			rp.BoughtTogetherItems = append(rp.BoughtTogetherItems, &bt)
		}
	}
	return rp, nil
}
func convertScore(s string) int {
	r, er := strconv.ParseInt(s, 0, 64)
	if er == nil {
		return int(r)
	} else {
		glog.V(2).Infof("failed to parseInt %s\n", er.Error())
		return 0
	}
}
func convertToScoreByRegion(l []*dynamodb.AttributeValue) catalog.RegionScores {
	result := &catalog.RegionScores{}
	for _, v := range l {
		s := &catalog.RegionScore{
			Region: v.M["Region"].S,
			Score:  convertScore(*v.M["Score"].N),
		}
		*result = append(*result, s)
	}
	return *result
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
	}
	resp, err := svc.GetItem(params)

	if err != nil {
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
	data      *recommendation.RelatedProducts
}

func (relates *RawHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	glog.V(2).Infof("serving %s", r.URL.Path)
	param := GetSearchParams(r, relates.pathBound)
	var prod recommendation.Product
	relatesProducts := *relates.data
	if val, ok := relatesProducts.Relates[param.ProductId]; ok {
		prod = *val
	} else {
		prod = recommendation.Product{
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
	data      *recommendation.RelatedProducts
}

func (relates *GeoHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	glog.V(2).Infof("serving %s", r.URL.Path)
	param := GetSearchParams(r, relates.pathBound)
	var prod recommendation.Product
	relatesProducts := *relates.data
	if val, ok := relatesProducts.Relates[param.ProductId]; ok {
		prod = *val
	} else {
		prod = recommendation.Product{
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
	glog.Infof("data source is pointing to dynamo db. service ready on port 8080")
	glog.Fatal(http.ListenAndServe(":8080", mux))
}

func serveFromS3(s3Location string) {
	var relatedProducts recommendation.RelatedProducts = recommendation.RelatedProducts{Relates: make(map[string]*recommendation.Product)}

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

func parallelLoadS3(relates *recommendation.RelatedProducts, svc *s3.S3, dataDir string) {
	bucket, keypattern := parseS3Params(dataDir)
	params := &s3.ListObjectsInput{
		Bucket: aws.String("ecomm-order-items"), // Required
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())
	}

	workQueue := make(chan Payload, 25)
	statusQueue := make(chan recommendation.RelatedProducts, 25)

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

func processWorkQueue(q chan Payload, s chan recommendation.RelatedProducts) {
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
	RelatedItems *recommendation.RelatedProducts
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
					aws.String("NumOfTxn"),
					aws.String("SimilarItems"),
					aws.String("AvailableSkus"),
					aws.String("SalesByRegion"),
					aws.String("Price"),
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
		p.ProductId=*i["productId"].S
		for a, v := range i {
			switch a {
			//case "productId":
			//	p.ProductId = *v.S
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
				p.ParentCat = *v.S
			case "Price":
				if p.PriceRange==nil{
					p.PriceRange=make(map[string]*catalog.Price)
				}
				var err error
				for c,price:=range v.M{
					mp:=catalog.Price{CurrencyCode:c}
					mp.ListPrice,err=strconv.ParseFloat(*price.M["ListPrice"].N,64)
					if err!=nil{
						glog.V(2).Infof("failed to get produrct[%s] list price %s\n",p.ProductId,err.Error())
					}
					mp.SalePrice,err=strconv.ParseFloat(*price.M["SalePrice"].N,64)
					if err!=nil{
						glog.V(2).Infof("failed to get produrct[%s] sale price %s\n",p.ProductId,err.Error())
					}
					p.PriceRange[c]=&mp
					glog.V(4).Infof("product[%s] is on sale %v in %s \n",p.ProductId,p.IsOnSale(c),c)
				}
			case "NumOfTxn":
				p.NumOfPurchase = convertScore(*v.N)
			case "AvailableSkus":
				if len(v.SS) > 0 {
					p.Availability = true
				} else {
					p.Availability = false
				}
			case "SimilarItems":
				for _, i := range v.M {
					for k, v := range i.M {
						if k == "ProductId" {
							p.SimilarItems = append(p.SimilarItems, v.S)
						}
					}
				}
			case "SalesByRegion":
				for _, i := range v.L {
					rs := &catalog.RegionScore{}
					for k, v := range i.M {
						if k == "Region" {
							rs.Region = v.S
						} else if k == "Score" {
							rs.Score = convertScore(*v.N)
						}
					}
					p.SalesByRegion = append(p.SalesByRegion, rs)

				}

			}
		}

		result[p.ProductId] = p
	}
	return result
}
