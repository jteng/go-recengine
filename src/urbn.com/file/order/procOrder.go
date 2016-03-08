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
	State string
	Items  []Item
}

type Item struct{
	ProductId string
	SkuId string
	Color string
	State string
}
func main(){
	fileName := flag.String("file", "", "file to be parsed")
	coloredItems:=flag.Bool("coloredItem",false,"colored item indicator")
	geodItems:=flag.Bool("geoedItem",false,"geo awared item indicator")
	flag.Parse()
	if *coloredItems {
		processColoredItems(*fileName)
		return
	}else if *geodItems {
		processGeoAwareItems(*fileName)
		return
	}else{
		processPlainProducts(*fileName)
	}

	fmt.Println("done")


}

func processPlainProducts(fileName string){
	orderItems := make(map[string]OrderItems)
	contents, _ := ioutil.ReadFile(fileName)
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

}
//convert these to
//"V1017153288"	"37418258"	"BLACK"
//"V1017153288"	"36022135"	"NUDE"
//"V1017153288"	"36022135"	"BLACK"

//37418258-BLACK 36022135-NUDE
//37418258-BLACK 36022135-BLACK
//36022135-NUDE 36022135-BLACK
//36022135-NUDE 37418258-BLACK
//36022135-BLACK 37418258-BLACK
//36022135-BLACK 36022135-BLACK


func processColoredItems(fileName string){
	orderItems := make(map[string]OrderItems)
	contents, _ := ioutil.ReadFile(fileName)
	strContent := string(contents[:])
	lines := strings.Split(strContent, "\n")
	for i:=0;i<len(lines);i++{
		parts := strings.Split(lines[i],"\t")
		if len(parts)<3{
			continue
		}else{
			orderId :=strings.Trim(parts[0],"\"");
			productId :=strings.Trim(parts[1],"\"")
			color :=strings.Trim(parts[2],"\"")
			item :=Item{ProductId:productId,Color:color}

			order,ok := orderItems[orderId]
			if !ok {
				order=OrderItems{OrderId:orderId}
			}
			order.Items=append(order.Items, item)
			orderItems[order.OrderId]=order
		}


	}

	fo, err := os.Create("/Users/tengj6/Downloads/flatOrderColoredItems.txt")
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
		for _,item :=range val.Items{
			for _,inner:=range val.Items{
				if item.ProductId!=inner.ProductId && item.Color!=inner.Color{
					fo.WriteString(item.ProductId+","+item.Color+","+inner.ProductId+"-"+inner.Color+"\n")
				}
			}
		}
	}

}

func processGeoAwareItems(fileName string){
	fmt.Println("processing geo awared items.")
	orderItems := make(map[string]OrderItems)
	contents, _ := ioutil.ReadFile(fileName)
	strContent := string(contents[:])
	lines := strings.Split(strContent, "\n")
	for i:=0;i<len(lines);i++{
		parts := strings.Split(lines[i],"\t")
		if len(parts)<3{
			continue
		}else{
			orderId :=strings.Trim(parts[0],"\"");
			state :=strings.Trim(parts[1],"\"")
			productId :=strings.Trim(parts[2],"\"")

			item :=Item{ProductId:productId,State:state}

			order,ok := orderItems[orderId]
			if !ok {
				order=OrderItems{OrderId:orderId,State:state}
			}
			order.Items=append(order.Items, item)
			orderItems[order.OrderId]=order
		}


	}

	fo, err := os.Create("/Users/tengj6/Downloads/flatOrderGeoAwaredItems.txt")
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
		skipLine:=false
		if len(val.Items)>=2{
			for _,item :=range val.Items{
				if item.State==""{
					skipLine=true
					continue
				}
				if !first {
					fo.WriteString(","+item.ProductId)
				}else{
					fo.WriteString(item.ProductId)
					first=false
				}
				//fmt.Printf(",%s",item.ProductId)
			}
			if !skipLine{
				fo.WriteString(","+val.State+"\n")
			}else{
				skipLine=false
			}
		}
	}

}


