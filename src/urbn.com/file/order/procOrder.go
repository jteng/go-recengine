package main
import (
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"os"
)


type OrderItems struct {
	OrderId    string
	Items  []Item
}

type Item struct{
	ProductId string
	SkuId string
}
func main(){
	fileName := flag.String("file", "", "file to be parsed")
	flag.Parse()
	orderItems := make(map[string]OrderItems)
	contents, _ := ioutil.ReadFile(*fileName)
	strContent := string(contents[:])
	lines := strings.Split(strContent, "\n")
	for i:=0;i<len(lines);i++{
		parts := strings.Split(lines[i],"\t")
		if len(parts)<3{
			continue
		}else{
			orderId :=strings.Trim(parts[0],"\"");
			productId :=strings.Trim(parts[1],"\"")
			skuId :=strings.Trim(parts[2],"\"")
			item :=Item{ProductId:productId,SkuId:skuId}

			order,ok := orderItems[orderId]
			if !ok {
				order=OrderItems{OrderId:orderId}
			}
			order.Items=append(order.Items, item)
			orderItems[order.OrderId]=order
		}


	}

	fo, err := os.Create("/Users/tengj6/Downloads/flatOrderItems.txt")
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()



	for _,val := range orderItems{
		//fo.WriteString(val.OrderId)
		//fmt.Printf("%s",val.OrderId)
		first :=true
		for _,item :=range val.Items{
			if !first {
				fo.WriteString(","+item.ProductId)
			}else{
				fo.WriteString(item.ProductId)
				first=false
			}
			//fmt.Printf(",%s",item.ProductId)
		}
		fo.WriteString("\n")
	}

	fmt.Println("done")


}

