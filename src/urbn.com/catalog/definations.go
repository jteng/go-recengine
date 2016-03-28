package catalog

import (
	"encoding/xml"
	"github.com/golang/glog"
	"math"
	"sort"
)

type Availability int

const (
	InStock     Availability = iota // 0
	BackOrdered                     // 1
	PreOrder                        // 2
	OutOfStock                      // 3
)
const (
	CURRENCY_CODE_CAD = "CAD"
	CURRENCY_CODE_USD = "USD"
)
const (
	SIMILAR_PRODUCTS_THREADHOLD = 30
	MAX_SALES_BY_REGION=30
)

type ProductCatalog struct {
	Categories map[string]*Category
	Products   map[string]*Product
	Skus       map[string]*Sku
}
type Feed struct {
	XMLName    xml.Name    `xml:"Feed"`
	Categories *Categories `xml:"Categories"`
	Products   *Products   `xml:"Products"`
	Skus       *Skus       `xml:"Skus"`
}
type Categories struct {
	XMLName xml.Name    `xml:"Categories"`
	Items   []*Category `xml:"Category"`
}
type Category struct {
	XMLName       xml.Name `xml:"Category"`
	CategoryId    string   `xml:"CategoryId"`
	ParentCat     string   `xml:"ParentCategoryId"`
	CategoryName  string   `xml:"CategoryName"`
	ChildProducts SortableProducts
	Sorted        bool
}

type Products struct {
	XMLName xml.Name   `xml:"Products"`
	Items   []*Product `xml:"Product"`
}
type Product struct {
	XMLName       xml.Name `xml:"Product"`
	ProductId     string   `xml:"ProductId"`
	ProductName   string   `xml:"ProductName"`
	Desc          string   `xml:"Description"`
	ParentCat     string   `xml:"ParentCategory"`
	ChildSkus     []*Sku
	LeadColor     string `xml:"LeadColor"`
	Catalog       *ProductCatalog
	ImageUrl      string
	ImageUrls     map[string]*string
	NumOfPurchase int
	SalesByRegion RegionScores
	SimilarItems  []*string
	Availability  bool
}

type RegionScore struct {
	Region *string
	Score  int
}

func (r *RegionScore) Merge(n RegionScore) {
	if r.Region == n.Region {
		glog.V(4).Infof("region[%s] matches sum the score\n", r.Region)
		r.Score = r.Score + n.Score
	}
}

type RegionScores []*RegionScore

func (p RegionScores) Len() int {
	return len(p)
}
func (p RegionScores) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p RegionScores) Less(i, j int) bool {
	return p[i].Score > p[j].Score
}

func (r *RegionScores) Merge(n RegionScores) {
	for _, v := range n {
		e, pos := r.Search(*v)
		if pos != -1 {
			glog.V(4).Infof("merging region score %s  %d %d \n", e.Region, e.Score, v.Score)
			e.Merge(*v)
			glog.V(4).Infof("merged region score %s  %d \n", e.Region, e.Score)
			//r[pos]=*e
		} else {
			glog.V(4).Infof("appending region score %s  \n", v.Region)
			*r = append(*r, v)
		}
	}
}

func (r RegionScores) Search(n RegionScore) (*RegionScore, int) {
	for i, v := range r {
		if *v.Region == *n.Region {
			return v, i
		}
	}
	return nil, -1
}

func (p RegionScores) TopN(n int) []*RegionScore {
	sort.Sort(p)
	t := math.Min(float64(n), float64(len(p)))
	return p[:(int64(t))]

}

func (p Product) GetPreferredColor(cs []*string) *string {
	for _, c := range cs {
		v, ok := p.ImageUrls[*c]
		if ok {
			glog.V(4).Infof("found preferred color %s in product[%s]\n", *c, p.ProductId)
			return v
		}
	}
	glog.V(5).Infof("return lead color as the  preferred color for product[%s]\n", p.ProductId)
	return &p.ImageUrl
}
func (p Product) IsAvailable() bool {
	return len(p.GetAvailableSkus()) > 0
}

//return price low and price high
func (p Product) GetPriceRange(currencyCode string) (float64, float64) {
	var low, high float64 = 0, 0
	for _, s := range p.ChildSkus {
		if s.Availability != OutOfStock {
			v, ok := s.Price[currencyCode]
			if ok {
				if low == 0 || low > v {
					low = v
				}
				if high == 0 || high < v {
					high = v
				}
			}
		}

	}
	return low, high
}
func (p Product) GetPrice() map[string]Price {
	result := make(map[string]Price)
	for _, s := range p.ChildSkus {
		if s.Availability != OutOfStock {
			for c, prc := range s.Price {
				v, ok := result[c]
				if ok {
					if v.SalePrice > prc {
						v.SalePrice = prc
					} else if v.ListPrice < prc {
						v.ListPrice = prc
					}
					result[c] = v
				} else {
					result[c] = Price{
						ListPrice:    prc,
						SalePrice:    prc,
						CurrencyCode: c,
					}
				}
			}
		}

	}
	return result
}

//return the lead color thumbnail img url or a random one...
func (p Product) GetLeadColorImagelUrl() string {

	var result string
	for _, s := range p.ChildSkus {
		if s.Availability != OutOfStock {
			result = s.CatImageUrl //s.ThumbnailImageUrls.ImageUrl[0]
			if s.ColorCode == p.LeadColor {
				return result
			}
		}
	}
	return result
}

func (p Product) GetImageUrls() map[string]string {
	result := make(map[string]string)
	for _, s := range p.ChildSkus {
		if s.Availability != OutOfStock {
			_, u := result[s.Color]
			if !u {
				result[s.Color] = s.CatImageUrl
			}
		}
	}
	return result
}

//return a set of childsku ids
func (p Product) GetChildSkus() []string {
	var result []string
	for _, s := range p.ChildSkus {
		result = append(result, s.SkuId)
	}
	return result
}

//return available skus
func (p Product) GetAvailableSkus() []string {
	var result []string
	for _, s := range p.ChildSkus {
		if s.Availability != OutOfStock {
			result = append(result, s.SkuId)
		}
	}
	return result
}

func (p Product) GetAvailableColors() []string {
	var result []string
	set := make(map[string]bool)
	for _, s := range p.ChildSkus {
		if s.Availability != OutOfStock {
			if set[s.Color] {
				continue
			}
			result = append(result, s.Color)
			set[s.Color] = true
		}
	}
	return result
}

func (p Product) GetAvailableSizes() []string {
	var result []string
	set := make(map[string]bool)
	for _, s := range p.ChildSkus {
		if s.Availability != OutOfStock {
			if set[s.Size] {
				continue
			}
			result = append(result, s.Size)
			set[s.Size] = true
		}
	}
	return result
}
func (p Product) GetSimilarProducts() map[int]*Product {
	result := make(map[int]*Product)
	pc, ok := p.Catalog.Categories[p.ParentCat]
	if ok {
		if !pc.Sorted {
			sort.Sort(pc.ChildProducts)
			pc.Sorted = true
		}

		count := 0
		for _, sp := range pc.ChildProducts {
			if count >= SIMILAR_PRODUCTS_THREADHOLD {
				break
			}
			if sp.ProductId != p.ProductId && sp.IsAvailable() {
				//_, hi := sp.GetPriceRange(CURRENCY_CODE_USD)
				result[count] = sp //+ "_" + strconv.FormatFloat(hi, 'f', 2, 64)
				//result = append(result, sp.ProductId+"_"+strconv.FormatFloat(hi,'f', 2, 64))
				count++
			}
		}
	}
	return result
}

type Skus struct {
	XMLName xml.Name `xml:"Skus"`
	Items   []*Sku   `xml:"Sku"`
}
type Sku struct {
	XMLName            xml.Name           `xml:"Sku"`
	SkuId              string             `xml:"SkuId"`
	Color              string             `xml:"SkuColor"`
	ColorCode          string             `xml:"ColorCode"`
	Size               string             `xml:"SkuSize"`
	ParentProd         string             `xml:"ParentProductExternalId"`
	ThumbnailImageUrls ThumbNailImageUrls `xml:"ThumbNailImageUrlsBySku"`
	USDListPrice       float64            `xml:"USDListPrice"`
	CADListPrice       float64            `xml:"CADListPrice"`
	CatImageUrl        string             `xml:"ImageURL-CategoryPage"`
	Price              map[string]float64
	Availability       Availability
}
type ThumbNailImageUrls struct {
	XMLName  xml.Name `xml:"ThumbNailImageUrlsBySku"`
	ImageUrl []string `xml:"ThumbNailImageUrl"`
}
type Price struct {
	CurrencyCode string
	ListPrice    float64
	SalePrice    float64
}

type SortableProducts []*Product

func (p SortableProducts) Len() int {
	return len(p)
}
func (p SortableProducts) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p SortableProducts) Less(i, j int) bool {
	if p[i].NumOfPurchase != p[j].NumOfPurchase {
		return p[i].NumOfPurchase > p[j].NumOfPurchase
	} else {
		_, hi := p[i].GetPriceRange(CURRENCY_CODE_USD)
		_, hj := p[j].GetPriceRange(CURRENCY_CODE_USD)
		return hi > hj
	}
}
func (c Category) GetChildProducts(avfilter bool) []string {
	var result []string
	for _, p := range c.ChildProducts {
		if avfilter {
			if p.IsAvailable() {
				result = append(result, p.ProductId)
			}
		} else {
			result = append(result, p.ProductId)
		}
	}
	return result
}
