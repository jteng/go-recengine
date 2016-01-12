package main

import (
	"flag"
	"net/http"
	"strings"
	"encoding/json"
	"io/ioutil"
	"github.com/golang/glog"
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


func main(){
	dataDir:=flag.String("dataLocation","","")
	flag.Parse()
	glog.V(2).Infof("data dir is %s \n",*dataDir)
	mux := http.NewServeMux()
	relatedProducts:=GetRelatedProducts(*dataDir)
	myHandler := &relatedProducts
	mux.Handle("/recommendation/", myHandler)
	glog.Infof("servic ready")
	glog.Fatal(http.ListenAndServe(":7777", mux))

}