package customer

type Gender string

const (
	Gender_Male    Gender = "M"
	Gender_Female  Gender = "F"
	Gender_Unknown Gender = "U"
)

type Customer struct {
	CustomerId string
	Gender     Gender
	BrandPrefs []*string
	ColorPrefs []*string
	SizePrefs  []*string
}
