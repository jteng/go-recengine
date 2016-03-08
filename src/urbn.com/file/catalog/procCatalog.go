package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
	"urbn.com/catalog"
)


const (
	DYNAMO_WRITE_THRESHOLD = 25
	DYNAMO_READ_THRESHOLD  = 100
)
const (
	WRITE_CAPACITY_BURST   int64 = 300
	WRITE_CAPACITY_REGULAR int64 = 10
)

func loadCatalog(svc *s3.S3, file string, s3 bool) catalog.Feed {
	result := catalog.Feed{}

	var content []byte
	var error error
	if !s3 {
		content, error = ioutil.ReadFile(file + "/urban-master-feed.xml")
	} else {
		content, error = getObject(svc, "ecomm-catalog-inventory", "urban-master-feed.xml")
	}

	if error != nil {
		glog.V(1).Infof("failed to read file %s\n", file)
	}

	error = xml.Unmarshal(content, &result)
	if error != nil {
		glog.V(1).Infof("failed to unmarsh xml %s \n", content)
	}
	for idx, sku := range result.Skus.Items {
		if sku.Price == nil {
			sku.Price = make(map[string]float64)
		}
		if sku.CADListPrice > 0 {
			sku.Price[catalog.CURRENCY_CODE_CAD] = sku.CADListPrice
		}
		if sku.USDListPrice > 0 {
			sku.Price[catalog.CURRENCY_CODE_USD] = sku.USDListPrice
		}
		result.Skus.Items[idx] = sku
	}
	return result
}

func loadInventory(svc *s3.S3, file string, s3 bool) map[string]catalog.Availability {
	result := make(map[string]catalog.Availability)
	var content []byte
	var error error
	if !s3 {
		content, error = ioutil.ReadFile(file + "/inventory.csv")
	} else {
		content, error = getObject(svc, "ecomm-catalog-inventory", "inventory.csv")
	}
	if error != nil {
		glog.V(1).Infof("failed to load file %s", file)
		return nil
	}
	contentStr := string(content)
	parts := strings.Split(contentStr, "\n")
	for _, line := range parts {
		lps := strings.Split(line, ",")
		if len(lps) < 6 {
			continue
		}
		inv := catalog.OutOfStock
		stock, _ := strconv.ParseInt(strings.TrimSpace(lps[1]), 10, 64)
		bo, _ := strconv.ParseInt(strings.TrimSpace(lps[2]), 10, 64)
		if stock > 0 {
			inv = catalog.InStock
		} else if bo > 0 && lps[4] == "Y" {
			inv = catalog.BackOrdered
		}
		result[lps[0]] = inv
	}
	return result
}
func main() {
	dataDir := flag.String("dataLocation", "", "")
	useS3 := flag.Bool("useS3", false, "use s3 indicator")
	flag.Parse()
	svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	history := make(map[time.Time]bool)

	if !*useS3 {
		processFeeds(svc, *dataDir, *useS3)
		return
	}

	for true {
		time.Sleep(2 * time.Second)
		if isFeedReady(svc, history) {
			glog.V(1).Infof("feed is ready, process...")
			processFeeds(svc, *dataDir, *useS3)
		}
	}
}
func processFeeds(svc *s3.S3, dataDir string, useS3 bool) {

	glog.V(1).Infof("starting to load catalog file... \n")
	//feed := loadCatalog("/Users/tengj6/Downloads/urban-master-feed.xml")
	feed := loadCatalog(svc, dataDir, useS3)
	glog.V(1).Infof("finished load catalog.\nstart to load inventory file...\n")
	inv := loadInventory(svc, dataDir, useS3)
	glog.V(1).Infof("finished load inventory \n")

	catalog := catalog.ProductCatalog{
		Categories: make(map[string]*catalog.Category),
		Products:   make(map[string]*catalog.Product),
		Skus:       make(map[string]*catalog.Sku),
	}
	//populate catalog
	for _, c := range feed.Categories.Items {
		catalog.Categories[c.CategoryId] = c
	}

	//populate category.ChildProducts
	for _, p := range feed.Products.Items {
		p.Catalog = &catalog
		catalog.Products[p.ProductId] = p
		v, ok := catalog.Categories[p.ParentCat]
		if ok {
			v.ChildProducts = append(v.ChildProducts, p)
		} else {
			//catalog.Categories[p.ParentCat]=
			glog.V(2).Infof("category %s isn't in the categories list\n", p.ParentCat)
		}
	}
	//populate childSkus for each product
	for _, sku := range feed.Skus.Items {
		i, cool := inv[sku.SkuId]
		if cool {
			sku.Availability = i
		}
		p, ok := catalog.Products[sku.ParentProd]
		if ok {
			p.ChildSkus = append(p.ChildSkus, sku)
		} else {
			glog.V(2).Infof("sku %s is orphan \n", sku.SkuId)
		}
	}

	dsvc := dynamodb.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	bulkWriteProducts(dsvc, catalog)
	glog.V(1).Infof("finished update products. \n")
	//bulkWriteCategories(dsvc, catalog)
	glog.V(1).Infof("finished update categories. \n")

}

func bulkWriteProducts(svc *dynamodb.DynamoDB, pctlg catalog.ProductCatalog) {
	//UpdateTableCapacity("UoProducts",svc, 10, WRITE_CAPACITY_BURST)
	//defer UpdateTableCapacity("UoProducts",svc, 10, WRITE_CAPACITY_REGULAR)

	pwqs := makeProductWriteRequests(&pctlg)
	var params = &dynamodb.BatchWriteItemInput{}
	params.RequestItems = make(map[string][]*dynamodb.WriteRequest)
	count := 0
	var batch []*dynamodb.WriteRequest
	for i := 0; i < len(pwqs); i++ {
		batch = append(batch, pwqs[i])
		count = count + 1
		if count >= DYNAMO_WRITE_THRESHOLD {
			params.RequestItems["UoProducts"] = batch
			resp, err := svc.BatchWriteItem(params)
			if err != nil {
				glog.Errorf("failed to write to dynamo %v\n", err.Error())
			}
			//fmt.Println(resp)
			upi := resp.UnprocessedItems
			for len(upi) > 0 {
				params.RequestItems = upi
				resp, err := svc.BatchWriteItem(params)
				if err != nil {
					fmt.Println("\n" + err.Error())
				}
				upi = resp.UnprocessedItems
			}
			count = 0
			batch = batch[:0]
			params.RequestItems = make(map[string][]*dynamodb.WriteRequest)
			glog.V(1).Infof("finished product batch %d\n", i/DYNAMO_WRITE_THRESHOLD)
		}
	}
	if len(batch) > 0 {
		params.RequestItems["UoProducts"] = batch
		resp, err := svc.BatchWriteItem(params)
		if err != nil {
			glog.Errorf("failed to write to dynamo %v\n", err.Error())
		}
		upi := resp.UnprocessedItems
		for len(upi) > 0 {
			params.RequestItems = upi
			resp, err := svc.BatchWriteItem(params)
			if err != nil {
				fmt.Println("\n" + err.Error())
			}
			upi = resp.UnprocessedItems
		}
	}

}
/*
func WriteProductToDynamoDb(svc *dynamodb.DynamoDB, product catalog.Product) string {
	glog.V(2).Infof("productId is %s\n", product)
	pms := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{ // Required
			"productId": { // Required
				S: aws.String(product.ProductId),
			},
			// More values...
		},
		TableName: aws.String("UoProducts"), // Required
		AttributeUpdates: map[string]*dynamodb.AttributeValueUpdate{
			"ProductName": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					S: aws.String(product.ProductName),
				},
			},
			"ParentCategory": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					S: aws.String(product.ParentCat),
				},
			},
			"LeadColor": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					S: aws.String(product.LeadColor),
				},
			},
			"LeadColorThumbnailUrl": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					S: aws.String(product.GetLeadColorImagelUrl()),
				},
			},
			"ChildSkus": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					SS: aws.StringSlice(product.GetChildSkus()),
				},
			},
			"AvailableSkus": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					SS: aws.StringSlice(product.GetAvailableSkus()),
				},
			},
			"Price": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					M: convertPriceToDynamoValue(product.GetPrice()),
				},
			},
			"ThumbnailUrls": {
				Action: aws.String("PUT"),
				Value: &dynamodb.AttributeValue{
					M: convertImageUrlsToDynamoValue(product.GetImageUrls()),
				},
			},
		},
	}

	resp, err := svc.UpdateItem(pms)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.Errorln(err.Error())
		return "failed to query DynamoDb"
	}

	// Pretty-print the response data.
	glog.V(1).Infof("resp: %v\n", resp)

	return ""
}
*/
func convertPriceToDynamoValue(prc map[string]catalog.Price) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	for c, p := range prc {
		result[c] = &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"ListPrice": { // Required
					N: aws.String(strconv.FormatFloat(p.ListPrice, 'E', -1, 64)),
				},
				"SalePrice": { // Required
					N: aws.String(strconv.FormatFloat(p.SalePrice, 'E', -1, 64)),
				},
			},
		}
	}
	return result
}

func convertSortedProductsToDynamoValue(prc map[int]string) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	for c, p := range prc {
		result[strconv.FormatInt(int64(c), 10)] = &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"ProductId": { // Required
					S: aws.String(p),
				},
			},
		}
	}
	return result
}

func convertImageUrlsToDynamoValue(urls map[string]string) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	for c, u := range urls {
		result[c] = &dynamodb.AttributeValue{
			S: aws.String(u),
		}
	}
	return result
}

func makeProductWriteRequests(catalog *catalog.ProductCatalog) []*dynamodb.WriteRequest {
	var result []*dynamodb.WriteRequest
	for _, p := range catalog.Products {
		item := convertProductToDynamo(p)
		pr := &dynamodb.PutRequest{
			Item: item,
		}
		wr := &dynamodb.WriteRequest{
			PutRequest: pr,
		}
		result = append(result, wr)
	}
	return result
}

func makeCategoryWriteRequests(catalog *catalog.ProductCatalog) []*dynamodb.WriteRequest {
	var result []*dynamodb.WriteRequest
	for _, c := range catalog.Categories {
		item := convertCategoryToDynamo(c)
		pr := &dynamodb.PutRequest{
			Item: item,
		}
		wr := &dynamodb.WriteRequest{
			PutRequest: pr,
		}
		result = append(result, wr)
	}
	return result
}

func convertProductToDynamo(p *catalog.Product) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	result["productId"] = &dynamodb.AttributeValue{
		S: aws.String(p.ProductId),
	}
	if p.LeadColor != "" {
		result["LeadColor"] = &dynamodb.AttributeValue{
			S: aws.String(p.LeadColor),
		}
	}
	lcu := p.GetLeadColorImagelUrl()
	if lcu != "" {
		result["LeadColorImageUrl"] = &dynamodb.AttributeValue{
			S: aws.String(lcu),
		}
	}
	if p.ProductName != "" {
		result["ProductName"] = &dynamodb.AttributeValue{
			S: aws.String(p.ProductName),
		}
	}
	if p.ParentCat != "" {
		result["ParentCategory"] = &dynamodb.AttributeValue{
			S: aws.String(p.ParentCat),
		}
	}

	if len(p.GetChildSkus()) > 0 {
		result["ChildSkus"] = &dynamodb.AttributeValue{
			SS: aws.StringSlice(p.GetChildSkus()),
		}
	}
	if len(p.GetAvailableSkus()) > 0 {
		result["AvailableSkus"] = &dynamodb.AttributeValue{
			SS: aws.StringSlice(p.GetAvailableSkus()),
		}
	}
	if len(p.GetAvailableColors()) > 0 {
		result["AvailableColors"] = &dynamodb.AttributeValue{
			SS: aws.StringSlice(p.GetAvailableColors()),
		}
	}
	if len(p.GetAvailableSizes()) > 0 {
		result["AvailableSizes"] = &dynamodb.AttributeValue{
			SS: aws.StringSlice(p.GetAvailableSizes()),
		}
	}
	if p.IsAvailable() {
		similarItems := p.GetSimilarProducts()
		dsi := convertSortedProductsToDynamoValue(similarItems)
		if len(similarItems) > 0 {
			result["SimilarItems"] = &dynamodb.AttributeValue{
				//SS: aws.StringSlice(similarItems),
				M: dsi,
			}
		}
	}
	price := convertPriceToDynamoValue(p.GetPrice())
	if len(price) > 0 {
		result["Price"] = &dynamodb.AttributeValue{
			M: price,
		}
	}
	urls := convertImageUrlsToDynamoValue(p.GetImageUrls())
	if len(urls) > 0 {
		result["ImageUrls"] = &dynamodb.AttributeValue{
			M: urls,
		}
	}

	return result
}

func convertCategoryToDynamo(cat *catalog.Category) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	result["categoryId"] = &dynamodb.AttributeValue{
		S: aws.String(cat.CategoryId),
	}
	if cat.CategoryName != "" {
		result["CategoryName"] = &dynamodb.AttributeValue{
			S: aws.String(cat.CategoryName),
		}
	}
	if cat.ParentCat != "" {
		result["ParentCategory"] = &dynamodb.AttributeValue{
			S: aws.String(cat.ParentCat),
		}
	}
	cps := cat.GetChildProducts(false)
	if len(cps) > 0 {
		result["ChildProducts"] = &dynamodb.AttributeValue{
			SS: aws.StringSlice(cps),
		}
	}
	acps := cat.GetChildProducts(true)
	if len(acps) > 0 {
		result["AvailableChildProducts"] = &dynamodb.AttributeValue{
			SS: aws.StringSlice(acps),
		}
	}
	return result
}

func UpdateTableCapacity(tbl string, svc *dynamodb.DynamoDB, r, w int64) {

	params := &dynamodb.UpdateTableInput{
		TableName: aws.String(tbl), // Required

		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(r), // Required
			WriteCapacityUnits: aws.Int64(w), // Required
		},
	}
	resp, err := svc.UpdateTable(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.Errorf("failed to update table[%s] capacity due to %v\n", tbl, err.Error())
		return
	}

	// Pretty-print the response data.
	if *resp.TableDescription.TableStatus == "UPDATING" {
		glog.V(1).Infof("capacity of table[%s] is updating", tbl)
	}
	//fmt.Println(resp)
}

func bulkWriteCategories(svc *dynamodb.DynamoDB, pctlg catalog.ProductCatalog) {
	//UpdateTableCapacity("UoCategories",svc, 10, WRITE_CAPACITY_BURST)
	//defer UpdateTableCapacity("UoCategories",svc, 10, WRITE_CAPACITY_REGULAR)

	pwqs := makeCategoryWriteRequests(&pctlg)
	var params = &dynamodb.BatchWriteItemInput{}
	params.RequestItems = make(map[string][]*dynamodb.WriteRequest)
	count := 0
	var batch []*dynamodb.WriteRequest
	for i := 0; i < len(pwqs); i++ {
		batch = append(batch, pwqs[i])
		count = count + 1
		if count >= DYNAMO_WRITE_THRESHOLD {
			params.RequestItems["UoCategories"] = batch
			resp, err := svc.BatchWriteItem(params)
			if err != nil {
				glog.Errorf("failed to write to dynamo %v\n", err.Error())
			}
			//fmt.Println(resp)
			upi := resp.UnprocessedItems
			for len(upi) > 0 {
				params.RequestItems = upi
				resp, err := svc.BatchWriteItem(params)
				if err != nil {
					fmt.Println("\n" + err.Error())
				}
				upi = resp.UnprocessedItems
			}
			count = 0
			batch = batch[:0]
			params.RequestItems = make(map[string][]*dynamodb.WriteRequest)
			glog.V(1).Infof("finished product batch %d\n", i/DYNAMO_WRITE_THRESHOLD)
		}
	}
	if len(batch) > 0 {
		params.RequestItems["UoCategories"] = batch
		resp, err := svc.BatchWriteItem(params)
		if err != nil {
			fmt.Println("\n" + err.Error())
		}
		upi := resp.UnprocessedItems
		for len(upi) > 0 {
			params.RequestItems = upi
			resp, err := svc.BatchWriteItem(params)
			if err != nil {
				glog.Errorf("failed to write to dynamo %v\n", err.Error())
			}
			upi = resp.UnprocessedItems
		}
	}

}

func isFeedReady(svc *s3.S3, history map[time.Time]bool) bool {
	params := &s3.ListObjectsInput{
		Bucket: aws.String("ecomm-catalog-inventory"), // Required
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())
	}

	for _, obj := range resp.Contents {
		glog.V(3).Infof("s3 object: %s. \n", *obj.Key)
		if "_SUCCESS" == *obj.Key {
			_, ok := history[*obj.LastModified]
			if !ok {
				history[*obj.LastModified] = true
				if len(history) == 1 {
					return false
				}
				return true
			} else {
				return false
			}

		}
	}

	return false
}

func getObject(svc *s3.S3, bucket string, key string) ([]byte, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket), // Required
		Key:    aws.String(key),    // Required
	}
	resp, err := svc.GetObject(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		//glog.Fatal(err.Error())
		return nil, err
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
	return bbuffer.Bytes(), nil
}
