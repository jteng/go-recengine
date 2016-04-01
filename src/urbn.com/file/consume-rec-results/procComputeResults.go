package main

import (
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"urbn.com/aws-s3"
	"urbn.com/catalog"
	"urbn.com/recommendation"
)

const (
	DYNAMO_WRITE_THRESHOLD = 25
	DYNAMO_READ_THRESHOLD  = 100
	BOUGHT_TOGETHER_BUCKET = "ecomm-order-items"
	MAX_CONCURRENT_UPDATES = 10
	MAX_CONCURRENT_READS   = 10
)

func main() {
	buckets := flag.String("bucket", "", "")
	flag.Parse()
	var parts []string
	if strings.Contains(*buckets, ",") {
		parts = strings.Split(*buckets, ",")
	}
	if len(parts) == 0 {
		parts = append(parts, *buckets)
	}
	for k, v := range parts {
		glog.V(1).Infof("bucket[%d]=%s\n", k, v)
	}
	svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	history := make(map[string]time.Time)
	init := true
	dsvc := dynamodb.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	for true {
		time.Sleep(5 * time.Second)
		for _, v := range parts {
			ok, key := isDataReady(svc, v, history, init)

			if ok {
				glog.V(1).Infof("bucket[%s] is ready with key[%s]\n", v, key)
				processResults(svc, dsvc, v, key)
			} else {
				glog.V(1).Infof("no new data in bucket %s\n", v)
			}
		}
		init = false
	}
}
func processResults(s3svc *s3.S3, dsvc *dynamodb.DynamoDB, bkt string, key string) {
	glog.V(2).Infof("process bucket[%s]/key[%s]\n", bkt, key)
	path := key[:strings.LastIndex(key, "_SUCCESS")]

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bkt), // Required

	}
	resp, err := s3svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())
	}

	for _, obj := range resp.Contents {
		glog.V(3).Infof("s3 object: %s. path:%s", *obj.Key, path)
		if strings.HasPrefix(*obj.Key, path+"part-") {
			glog.V(1).Infof("processing file %s \n", *obj.Key)
			existings := make(map[string]*recommendation.Product)
			rps := recommendation.RelatedProducts{}
			rps.PopulateRelatedProducts(aws_s3.GetObjectAsString(s3svc, bkt, *obj.Key))
			ids := make(map[string]bool) //[]*string
			for k, _ := range rps.Relates {
				glog.V(4).Infof("looping... product[%s]\n", k)
				_, ok := existings[k]
				if !ok {
					//ids = append(ids, &k)
					ids[k] = true
				}
			}
			glog.V(2).Infof("finished load results[%d]\n", len(rps.Relates))
			glog.V(4).Infof("ids %v", len(ids))
			glog.V(2).Infof("start load existing recommendations...\n")
			bulkRead(dsvc, ids, existings)
			glog.V(2).Infoln("fininished bulk read existing product recommendations.")
			glog.Flush()
			//merge results into existings
			for k, v := range rps.Relates {
				e, ok := existings[k]
				if ok {
					glog.V(4).Infof("product[%s] av=%d bt=%d\n", e.ProductID, len(e.AlsoViewedItems), len(e.BoughtTogetherItems))
					v.Merge(*e, bkt == BOUGHT_TOGETHER_BUCKET)
					glog.V(4).Infof("merged product=%s numOfPurchase=%d\n", v.ProductID, v.NumOfPurchase)
				} else {
					glog.V(4).Infof("added product=%s numOfPurchase=%d\n", v.ProductID, v.NumOfPurchase)
				}
			}

			//bulk update
			glog.V(2).Infof("bulk update product recommendations [%d]\n", len(existings))
			bulkWriteProductRecs(dsvc, rps)
			//updating numOfTxn and salesByRegion on each product
			ch := make(chan int, MAX_CONCURRENT_UPDATES)
			for _, p := range rps.Relates {
				go updateProductItem(dsvc, p.ProductID, p.NumOfPurchase, p.SalesByRegion, ch)

			}
			//update the category.childProduct
			glog.V(1).Infof("file %s is processed.\n", *obj.Key)
		}
	}

}

func bulkWriteProductRecs(svc *dynamodb.DynamoDB, rps recommendation.RelatedProducts) {
	pwqs := makeProdRecWriteRequests(rps)
	var params = &dynamodb.BatchWriteItemInput{}
	params.RequestItems = make(map[string][]*dynamodb.WriteRequest)
	count := 0
	var batch []*dynamodb.WriteRequest
	for i := 0; i < len(pwqs); i++ {
		batch = append(batch, pwqs[i])
		count = count + 1
		if count >= DYNAMO_WRITE_THRESHOLD {
			params.RequestItems["ProductRecommendation"] = batch
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
					glog.Errorf("error during bulk write prod consume-rec-results " + err.Error())
				}
				upi = resp.UnprocessedItems
			}
			count = 0
			batch = batch[:0]
			params.RequestItems = make(map[string][]*dynamodb.WriteRequest)
			glog.V(1).Infof("finished product recommendation batch %d\n", i/DYNAMO_WRITE_THRESHOLD)
			glog.Flush()
		}
	}
	if len(batch) > 0 {
		params.RequestItems["ProductRecommendation"] = batch
		resp, err := svc.BatchWriteItem(params)
		if err != nil {
			glog.Errorf("failed to write to dynamo %v\n", err.Error())
		}
		upi := resp.UnprocessedItems
		for len(upi) > 0 {
			params.RequestItems = upi
			resp, err := svc.BatchWriteItem(params)
			if err != nil {
				glog.Errorf("error during bulk write prod consume-rec-results " + err.Error())
			}
			upi = resp.UnprocessedItems
		}
	}
}
func makeProdRecWriteRequests(rps recommendation.RelatedProducts) []*dynamodb.WriteRequest {
	var result []*dynamodb.WriteRequest
	for _, p := range rps.Relates {
		item := convertProductRecToDynamo(p)
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
func convertProductRecToDynamo(p *recommendation.Product) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	result["productId"] = &dynamodb.AttributeValue{
		S: aws.String(p.ProductID),
	}
	glog.V(4).Infof("alsoviewed item length %d\n", len(p.AlsoViewedItems))
	if len(p.AlsoViewedItems) > 0 {
		result["avItems"] = &dynamodb.AttributeValue{
			M: convertAvToDynamo(&p.AlsoViewedItems),
		}
	}
	if len(p.BoughtTogetherItems) > 0 {
		result["btItems"] = &dynamodb.AttributeValue{
			M: convertBtToDynamo(&p.BoughtTogetherItems),
		}
	}

	return result
}
func convertAvToDynamo(avi *recommendation.Items) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	for _, i := range *avi {
		s := convertRegScoreToDynamo(i.ScoresByRegion)
		m := make(map[string]*dynamodb.AttributeValue)
		m["ProductId"] = &dynamodb.AttributeValue{
			S: aws.String(i.ProductID),
		}
		m["TotalScore"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(int64(i.TotalScore), 10)),
		}
		if len(s) > 0 {
			m["ScoreByRegion"] = &dynamodb.AttributeValue{
				L: s,
			}
		}
		result[i.ProductID] = &dynamodb.AttributeValue{
			M: m,
		}

	}
	glog.V(3).Infof("converted alsoviewedItems %d \n", len(result))
	return result
}
func convertBtToDynamo(avi *recommendation.Items) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue)
	for _, i := range *avi {
		s := convertRegScoreToDynamo(i.ScoresByRegion)
		m := make(map[string]*dynamodb.AttributeValue)
		m["ProductId"] = &dynamodb.AttributeValue{
			S: aws.String(i.ProductID),
		}
		m["TotalScore"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(int64(i.TotalScore), 10)),
		}
		if len(s) > 0 {
			m["ScoreByRegion"] = &dynamodb.AttributeValue{
				L: s,
			}
		}
		result[i.ProductID] = &dynamodb.AttributeValue{
			M: m,
		}

	}
	glog.V(3).Infof("converted boughtTogetherItems %d \n", len(result))
	return result
}
func convertRegScoreToDynamo(rs catalog.RegionScores) []*dynamodb.AttributeValue {
	var results []*dynamodb.AttributeValue
	for _, s := range rs {
		av := &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Region": { // Required
					S: aws.String(*s.Region),
				},
				"Score": {
					N: aws.String(strconv.FormatInt(int64(s.Score), 10)),
				},
			},
		}
		results = append(results, av)
	}
	return results
}
func bulkRead(dsvc *dynamodb.DynamoDB, ids map[string]bool, results map[string]*recommendation.Product) {
	glog.V(3).Info("bulk read now.")
	var keys []map[string]*dynamodb.AttributeValue
	for id, _ := range ids {
		m := make(map[string]*dynamodb.AttributeValue)
		m["productId"] = &dynamodb.AttributeValue{
			S: aws.String(id),
		}
		glog.V(4).Infof("add key: %s\n", id)
		keys = append(keys, m)
	}

	//
	//ch:=make(chan int,MAX_CONCURRENT_READS)
	ch := make(chan chan map[string]*recommendation.Product, MAX_CONCURRENT_READS)
	//input_ch := make(chan []map[string]*dynamodb.AttributeValue, DYNAMO_READ_THRESHOLD)
	//count := 0
	//batch_count := 0
	batch_num := int(len(keys) / DYNAMO_READ_THRESHOLD)
	if math.Mod(float64(len(keys)), float64(DYNAMO_READ_THRESHOLD)) != 0 {
		batch_num = batch_num + 1
	}
	glog.V(2).Infof("need %d batches to read.\n", batch_num)
	//	var batches [][]map[string]*dynamodb.AttributeValue
	//	var batch []map[string]*dynamodb.AttributeValue

	for i := 1; i <= batch_num; i++ {
		if i != batch_num {
			go handleBatchRead(dsvc, keys[(i-1)*DYNAMO_READ_THRESHOLD:i*DYNAMO_READ_THRESHOLD], i, ch)
		} else {
			go handleBatchRead(dsvc, keys[(i-1)*DYNAMO_READ_THRESHOLD:], i, ch)
		}
	}

	//glog.V(1).Infof("total batch = %d\n", batch_count)

	fn_count := 0
	for rstch := range ch {
		fn_count++
		glog.V(2).Infof("bulk read waiting for batch %d\n", fn_count)
		bt_rst := <-rstch
		for k, v := range bt_rst {
			results[k] = v
			glog.V(4).Infof("add prod[%s] from batch\n", k)
		}
		if fn_count >= batch_num {
			break
		}
	}

}

func handleBatchRead(dsvc *dynamodb.DynamoDB, batch []map[string]*dynamodb.AttributeValue, bn int, ch chan chan map[string]*recommendation.Product) {
	requestItems := make(map[string]*dynamodb.KeysAndAttributes)
	recItems := &dynamodb.KeysAndAttributes{
		AttributesToGet: []*string{
			aws.String("avItems"),
			aws.String("btItems"),
			aws.String("productId"),
			// More values...
		},
		ConsistentRead: aws.Bool(false),
	}
	rstchan := make(chan map[string]*recommendation.Product,1)

	recItems.Keys = batch
	/*
	for _, v := range recItems.Keys {
		glog.V(2).Infof("key in batch[%d] %s", bn, *v["productId"].S)
	}*/
	requestItems["ProductRecommendation"] = recItems
	params := &dynamodb.BatchGetItemInput{}
	results := make(map[string]*recommendation.Product)

	for {
		glog.V(3).Info("handle batch read")

		if params.RequestItems == nil {
			params.RequestItems = make(map[string]*dynamodb.KeysAndAttributes)
		}
		params.RequestItems["ProductRecommendation"] = recItems
		resp, err := dsvc.BatchGetItem(params)
		//<-ch
		if err != nil {
			glog.V(2).Info(err.Error())
		}
		glog.V(4).Infof("response %v\n", resp)

		items, ok := resp.Responses["ProductRecommendation"] //items is []map[string]*AttributeValue
		if ok {
			for _, attrvl := range items {
				prod := recommendation.Product{}
				for a, v := range attrvl {
					if "productId" == a {
						prod.ProductID = *v.S
					} else if "avItems" == a {
						for _, i := range v.M {
							m, ok := i.M["ScoreByRegion"]
							avi := recommendation.Item{
								ProductID:  *i.M["ProductId"].S,
								TotalScore: convertScore(*i.M["TotalScore"].N),
								//ScoresByRegion:convertToScoreByRegion(i.M["ScoreByRegion"].L),
							}
							if ok {
								avi.ScoresByRegion = convertToScoreByRegion(m.L)
							}

							prod.AlsoViewedItems = append(prod.AlsoViewedItems, &avi)
						}
					} else if "btItems" == a {
						for _, i := range v.M {
							m, ok := i.M["ScoreByRegion"]
							bt := recommendation.Item{
								ProductID:  *i.M["ProductId"].S,
								TotalScore: convertScore(*i.M["TotalScore"].N),
								//ScoresByRegion: convertToScoreByRegion(i.M["ScoreByRegion"].L),
							}
							if ok {
								bt.ScoresByRegion = convertToScoreByRegion(m.L)
							}
							prod.BoughtTogetherItems = append(prod.BoughtTogetherItems, &bt)
						}
					}
					prod.AlsoViewedItems = prod.AlsoViewedItems.TopN(recommendation.MAX_ITEMS)
					prod.BoughtTogetherItems = prod.BoughtTogetherItems.TopN(recommendation.MAX_ITEMS)
					if prod.ProductID!=""{
						results[prod.ProductID] = &prod
					}
				}
			}
		}
		if len(resp.UnprocessedKeys) > 0 {
			for tbl, kv := range resp.UnprocessedKeys {
				glog.V(3).Infof("process unprocessed keys for table %s key %s \n", tbl, kv)
				if tbl == "ProductRecommendation" {
					recItems.Keys = kv.Keys
				}
			}
			glog.V(2).Info("reprocess unprocessed keys...\n")
		} else {
			break
		}
	}
	glog.V(3).Infof("start to write result to rst_chan.\n")
	rstchan <- results
	glog.V(3).Infof("wrote result to rst_chan %d \n", len(results))
	ch <- rstchan
	glog.V(3).Infof("write result %d \n", len(results))

}

func convertScore(s string) int {
	r, er := strconv.ParseInt(s, 0, 64)
	if er == nil {
		return int(r)
	} else {
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
func isDataReady(svc *s3.S3, bkt string, history map[string]time.Time, init bool) (bool, string) {

	glog.V(3).Infof("init flag [%v]\n", init)
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bkt), // Required
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		glog.Errorf("%s, %s \n", err.(awserr.Error).Code(), err.(awserr.Error).Error())
	}

	for _, obj := range resp.Contents {
		glog.V(4).Infof("s3 object: %s. \n", *obj.Key)
		if strings.HasSuffix(*obj.Key, "_SUCCESS") {
			v, ok := history[bkt+"/"+*obj.Key]
			if !ok {
				history[bkt+"/"+*obj.Key] = *obj.LastModified
				if !init {
					return true, *obj.Key
				}

			} else {
				if v.Before(*obj.LastModified) {
					history[bkt+"/"+*obj.Key] = *obj.LastModified
					return true, *obj.Key
				}
			}
		}

	}

	return false, ""
}

//pass prodId and number of transactions
func updateProductItem(svc *dynamodb.DynamoDB, pid string, i int, salesByRgn catalog.RegionScores, ch chan int) {
	var rgnsales []*dynamodb.AttributeValue
	sort.Sort(salesByRgn)
	for _, r := range salesByRgn {
		dv := make(map[string]*dynamodb.AttributeValue)
		dv["Region"] = &dynamodb.AttributeValue{
			S: r.Region,
		}
		s := strconv.FormatInt(int64(r.Score), 10)
		dv["Score"] = &dynamodb.AttributeValue{
			N: &s,
		}
		v := &dynamodb.AttributeValue{
			M: dv,
		}
		rgnsales = append(rgnsales, v)
	}
	params := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{ // Required
			"productId": { // Required
				S: aws.String(pid),
			},
			// More values...
		},
		TableName: aws.String("UoProducts"), // Required
		AttributeUpdates: map[string]*dynamodb.AttributeValueUpdate{
			"NumOfTxn": { // Required
				Action: aws.String(dynamodb.AttributeActionPut),
				Value: &dynamodb.AttributeValue{
					N: aws.String(strconv.FormatInt(int64(i), 10)),
				},
			},
			// More values...
		},
	}
	if len(rgnsales) > 0 {
		params.AttributeUpdates["SalesByRegion"] = &dynamodb.AttributeValueUpdate{ // Required
			Action: aws.String(dynamodb.AttributeActionPut),
			Value: &dynamodb.AttributeValue{
				L: rgnsales,
			},
		}
	}
	ch <- 1
	_, err := svc.UpdateItem(params)
	<-ch
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		glog.V(1).Infof("failed to update product[%s] due to ", pid, err.Error())
		return
	} else {
		glog.V(3).Infof("updated numOfTxn of product[%s]", pid)
	}

}
