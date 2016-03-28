package recommendation

import (
	"encoding/json"
	"github.com/golang/glog"
	"math"
	"sort"
	"strings"
	"urbn.com/catalog"
)

const(
	MAX_ITEMS=30
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
	ProductID           string `json:"productId"`
	ParentCat           string
	ImageUrl            string
	NumOfPurchase       int   `json:"numOfPurchase"`
	BoughtTogetherItems Items `json:"boughtTogetherItems"`
	AlsoViewedItems     Items `json:"alsoViewedItems"`
	BestSimilar         Items
	SimilarItems        Items
	SalesByRegion       catalog.RegionScores `json:"salesByRegion"`
}

type RecItems struct {
	Items Items
}
type RecItemType string

const (
	REC_ITEM_TYPE_BOUGHTTOGETHER RecItemType = "B"
	REC_ITEM_TYPE_ALSOVIEWED     RecItemType = "V"
	REC_ITEM_TYPE_PICKED         RecItemType = "P"
	REC_ITEM_TYPE_SIMILAR        RecItemType = "S"
)

type Items []*Item

func (b *Items) merge(n Items) {
	glog.V(3).Infof("number of alsoViewed is %d\n", len(n))
	for _, ni := range n {
		i, ok := b.Search(*ni)
		if ok {
			glog.V(3).Infof("merge alsoviewed item %s\n", ni.ProductID)
			i.merge(*ni)
		} else {
			glog.V(3).Infof("add alsoviewed item %s\n", ni.ProductID)
			*b = append(*b, ni)
		}
	}
}
func (b Items) Search(c Item) (*Item, bool) {
	for _, i := range b {
		if i.ProductID == c.ProductID {
			return i, true
		}
	}
	return nil, false
}

func (p Items) Len() int {
	return len(p)
}
func (p Items) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p Items) Less(i, j int) bool {
	si := p[i].TotalScore
	sj := p[j].TotalScore
	if p[i].Availability == p[j].Availability {
		return si > sj
	} else if p[i].Availability {
		return true
	} else {
		return false
	}
}
func (p Items) TopN(f interface{}) []*Item {

	var pm *SearchParam
	var limit int
	pa, ok := f.(*SearchParam)
	if ok {
		pm = pa
		limit = pm.Limit
	} else {
		l, ok := f.(int)
		if ok {
			limit = l
		}
	}
	//var temp []*Item
	glog.V(4).Infoln("topN........")

	if pm != nil && pm.Location != "" {
		r := catalog.RegionScore{
			Region: &pm.Location,
		}
		loc := func(p1, p2 *Item) bool {
			if p1.Availability == p2.Availability {
				r1, i1 := p1.ScoresByRegion.Search(r)
				r2, i2 := p2.ScoresByRegion.Search(r)
				if i1 == -1 {
					r1 = &catalog.RegionScore{Region: &pm.Location, Score: 0}
				} else {
					glog.V(4).Infof("found region %s in %s\n", pm.Location, p1.ProductID)
				}
				if i2 == -1 {
					r2 = &catalog.RegionScore{Region: &pm.Location, Score: 0}
				} else {
					glog.V(4).Infof("found region %s in %s\n", pm.Location, p2.ProductID)
				}
				//glog.V(4).Infof("P[%s] is higher than p[%s] {%v}\n",p1.ProductID,p2.ProductID,r1.Score>r2.Score)
				if r1.Score != r2.Score {
					return r1.Score > r2.Score
				} else {
					return p1.TotalScore > p2.TotalScore
				}
			} else if p1.Availability {
				return true
			} else {
				return false
			}

		}

		By(loc).Sort(p)

	} else {
		sort.Sort(p)
		glog.V(4).Infof("null location \n")
	}
	t := math.Min(float64(limit), float64(len(p)))

	return p[:(int64(t))]

}

type Item struct {
	Type           RecItemType
	ProductID      string `json:"productId"`
	ProductName    string `json:"productName"`
	ImageUrl       string `json:"imageUrl"`
	TotalScore     int    `json:"totalScore"`
	ParentCat      string
	Availability   bool
	ScoresByRegion catalog.RegionScores `json:"scoreByRegion"`
}

func (bi *Item) merge(n Item) {
	bi.TotalScore = bi.TotalScore + n.TotalScore
	bi.ScoresByRegion.Merge(n.ScoresByRegion)
}

type By func(p1, p2 *Item) bool

func (by By) Sort(items []*Item) {
	ps := &itemSorter{
		items: items,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}

type itemSorter struct {
	items []*Item
	by    func(p1, p2 *Item) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (s *itemSorter) Len() int {
	return len(s.items)
}

// Swap is part of sort.Interface.
func (s *itemSorter) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *itemSorter) Less(i, j int) bool {
	return s.by(s.items[i], s.items[j])
}

func (p *Product) Merge(n Product, isBt bool) {
	if p.ProductID != n.ProductID {
		return
	}
	if !isBt {
		glog.V(3).Infof("merge bought together items [%d] \n", len(n.BoughtTogetherItems))
		p.BoughtTogetherItems.merge(n.BoughtTogetherItems)
		p.NumOfPurchase = p.NumOfPurchase + n.NumOfPurchase
	} else {
		glog.V(3).Infof("replace bought together items [%d]. \n", len(n.BoughtTogetherItems))
		//p.BoughtTogetherItems=n.BoughtTogetherItems
	}
	p.AlsoViewedItems.merge(n.AlsoViewedItems)

}

func (p Product) RemoveSelf() {
	var pos int
	for i, v := range p.AlsoViewedItems {
		if v.ProductID == p.ProductID {
			pos = i
			break
		}
	}
	if len(p.AlsoViewedItems) > pos+1 {
		p.AlsoViewedItems = append(p.AlsoViewedItems[:pos], p.AlsoViewedItems[pos+1:]...)
	}

	for i, v := range p.BoughtTogetherItems {
		if v.ProductID == p.ProductID {
			pos = i
			break
		}
	}
	if len(p.BoughtTogetherItems) > pos+1 {
		p.BoughtTogetherItems = append(p.BoughtTogetherItems[:pos], p.BoughtTogetherItems[pos+1:]...)
	}
}

type AlsoViewedItem Item

/*
type AlsoViewedItem struct {
	ProductID     string       `json:"productId"`
	ProductName   string       `json:"productName"`
	ImageUrl      string       `json:"imageUrl"`
	TotalScore    int          `json:"totalScore"`
	ScoreByRegion RegionScores `json:"scoreByRegion"`
}
*/
func (bi *AlsoViewedItem) merge(n AlsoViewedItem) {
	bi.TotalScore = bi.TotalScore + n.TotalScore
	bi.ScoresByRegion.Merge(n.ScoresByRegion)
}

type AlsoViewedItems []*AlsoViewedItem

func (b AlsoViewedItems) Search(c AlsoViewedItem) (*AlsoViewedItem, bool) {
	for _, i := range b {
		if i.ProductID == c.ProductID {
			return i, true
		}
	}
	return nil, false
}
func (b *AlsoViewedItems) merge(n AlsoViewedItems) {
	glog.V(3).Infof("number of alsoViewed is %d\n", len(n))
	for _, ni := range n {
		i, ok := b.Search(*ni)
		if ok {
			glog.V(3).Infof("merge alsoviewed item %s\n", ni.ProductID)
			i.merge(*ni)
		} else {
			glog.V(3).Infof("add alsoviewed item %s\n", ni.ProductID)
			*b = append(*b, ni)
		}
	}
}
func (p AlsoViewedItems) convertToItem() []*Item {

	result := make([]*Item, len(p))
	for i, v := range p {
		itm := Item(*v)
		result[i] = &itm
	}
	return result
}
func (p AlsoViewedItems) TopN(pm *SearchParam) []*AlsoViewedItem {
	var temp []*Item
	glog.V(3).Infoln("topN........")

	if pm.Location != "" {
		r := catalog.RegionScore{
			Region: &pm.Location,
		}
		loc := func(p1, p2 *Item) bool {
			r1, i1 := p1.ScoresByRegion.Search(r)
			r2, i2 := p2.ScoresByRegion.Search(r)
			if i1 == -1 {
				r1 = &catalog.RegionScore{Region: &pm.Location, Score: 0}
			} else {
				glog.V(4).Infof("found region %s in %s\n", &pm.Location, p1.ProductID)
			}
			if i2 == -1 {
				r2 = &catalog.RegionScore{Region: &pm.Location, Score: 0}
			} else {
				glog.V(4).Infof("found region %s in %s\n", &pm.Location, p2.ProductID)
			}
			//glog.V(4).Infof("P[%s] is higher than p[%s] {%v}\n",p1.ProductID,p2.ProductID,r1.Score>r2.Score)
			if r1.Score != r2.Score {
				return r1.Score > r2.Score
			} else {
				return p1.TotalScore > p2.TotalScore
			}

		}
		temp = p.convertToItem()
		By(loc).Sort(temp)
		for i, v := range temp {
			glog.V(5).Infof("[%d]=%s\n", i, v.ProductID)
		}
	} else {
		sort.Sort(p)
		glog.V(4).Infof("null location \n")
	}
	t := math.Min(float64(pm.Limit), float64(len(p)))
	if len(temp) > 0 {
		result := make([]*AlsoViewedItem, len(temp))
		for i, v := range temp {
			a := AlsoViewedItem(*v)
			result[i] = &a
		}
		for i, v := range result {
			glog.V(2).Infof("[%d]=%s\n", i, v.ProductID)
		}
		return result[:(int64(t))]
	} else {
		glog.V(2).Infof("temp is empty!!!!")
	}

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

type BoughtTogetherItem Item

/*
type BoughtTogetherItem struct {
	ProductID     string       `json:"productId"`
	ProductName   string       `json:"productName"`
	ImageUrl      string       `json:"imageUrl"`
	TotalScore    int          `json:"totalScore"`
	ScoreByRegion RegionScores `json:"scoreByRegion"`
}
*/
func (bi *BoughtTogetherItem) merge(n BoughtTogetherItem) {
	bi.TotalScore = bi.TotalScore + n.TotalScore
	bi.ScoresByRegion.Merge(n.ScoresByRegion)
}

type BoughtTogetherItems []*BoughtTogetherItem

func (b *BoughtTogetherItems) merge(n BoughtTogetherItems) {
	for _, ni := range n {
		i, ok := b.Search(*ni)
		if ok {
			i.merge(*ni)
		} else {
			*b = append(*b, ni)
		}
	}
}

func (b BoughtTogetherItems) Search(c BoughtTogetherItem) (*BoughtTogetherItem, bool) {
	for _, i := range b {
		if i.ProductID == c.ProductID {
			return i, true
		}
	}
	return nil, false
}
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
func (p BoughtTogetherItems) ConvertToItem() []*Item {
	result := make([]*Item, len(p))
	for i, v := range p {
		itm := Item(*v)
		result[i] = &itm
	}
	return result
}

//type RegionScore catalog.RegionScore
/*
struct {
	Region string `json:"region"`
	Score  int    `json:"score"`
}


type RegionScores []*catalog.RegionScore

func (r *RegionScores) merge(n RegionScores) {
	for _, v := range n {
		e, pos := r.search(*v)
		if pos != -1 {
			glog.V(4).Infof("merging region score %s  %d %d \n", e.Region, e.Score, v.Score)
			e.merge(*v)
			glog.V(4).Infof("merged region score %s  %d \n", e.Region, e.Score)
			//r[pos]=*e
		} else {
			glog.V(4).Infof("appending region score %s  \n", v.Region)
			*r = append(*r, v)
		}
	}
}

func (r RegionScores) search(n RegionScore) (*RegionScore, int) {
	for i, v := range r {
		if v.Region == n.Region {
			return v, i
		}
	}
	return nil, -1
}

func (r *RegionScore) merge(n RegionScore) {
	if r.Region == n.Region {
		glog.V(4).Infof("region[%s] matches sum the score\n", r.Region)
		r.Score = r.Score + n.Score
	}
}
*/
type RelatedProducts struct {
	Relates map[string]*Product
}

func (r *RelatedProducts) PopulateRelatedProducts(strContent string) {
	if r.Relates == nil {
		r.Relates = make(map[string]*Product)
	}
	//var result = RelatedProducts{Relates: make(map[string]Product)}
	parts := strings.Split(strContent, "\n")
	if strContent == "" || len(parts) == 0 {
		return
	}
	glog.V(3).Infof("number of results: %d", len(parts))
	for _, part := range parts {
		//get the json which starts from the first { and ends at the last )
		start := strings.Index(part, "{")
		end := strings.LastIndex(part, ")")
		if start < 0 || end >= len(part)||end<=0 {
			continue
		}

		jsonStr := part[start:end]

		var rp Product
		error := json.Unmarshal([]byte(jsonStr), &rp)
		if error != nil {
			glog.Errorf("failed to unmarshal json %s %s\n", jsonStr, error.Error())
		} else {
			r.Relates[rp.ProductID] = &rp
			glog.V(4).Infof("read product %s \n", rp.ProductID)
		}
	}
}

type SimilarItem struct {
	Type        RecItemType
	ProductID   string `json:"productId"`
	ProductName string `json:"productName"`
	ImageUrl    string `json:"imageUrl"`
	ParentCat   string
	NumOfTxn    int
	SalesByRegion catalog.RegionScores
}

type SimilarItems []*SimilarItem

func (p SimilarItems) Len() int {
	return len(p)
}
func (p SimilarItems) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p SimilarItems) Less(i, j int) bool {
	return p[i].NumOfTxn > p[j].NumOfTxn
}
func (p SimilarItems) TopN(n int) []*SimilarItem {
	sort.Sort(p)
	t := math.Min(float64(n), float64(len(p)))
	return p[:int64(t)]
}
