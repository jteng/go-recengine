package main
import (
	"fmt"
	"text/scanner"
	"strings"
	"net/url"
)

func main() {

	u, _ := url.Parse("http://bing.com/productId/123/color/red?q=dotnet")


	fmt.Printf("fragment %v \n",u.Fragment)
	fmt.Printf("path %v\n",u.Path)
	fmt.Printf("raw path %v \n",strings.ToLower(u.RawPath))
	fmt.Println(u)
	fmt.Printf("Query q is %s \n",u.Query().Get("q"))
	fmt.Printf("Query Q is %s \n",u.Query().Get("Q"))

	/*
	var result map[string]string = make(map[string]string)
	var s scanner.Scanner
	s.Init(strings.NewReader("productId/123/color/red"))
	var tok rune
	//var productId string
	for tok!=scanner.EOF{
		tok=s.Scan()
		v:=s.TokenText()
		if v!="/"{
			fieldName:=v

			value:=nextValue(s)
			fmt.Printf("add key:%s value:%s \n",fieldName,value)
			result[fieldName]=value
		}
	}
	for field,value:=range result{
		fmt.Printf("field is %s  value is %s \n",field,value)
	}
	*/
}

func nextValue(s scanner.Scanner) string{
	var result string
	for s.Scan()!=scanner.EOF{
		v:=s.TokenText()
		if v!="/"{
			fmt.Printf("tokenText is ",s.TokenText())
			result=v
			break
		}

	}
	return result
}
