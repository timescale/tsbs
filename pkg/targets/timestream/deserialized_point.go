package timestream

// deserializedPoint is a struct used by the Timestream
// loader to send data to the db. All the fields are strings
// because the Timestream SDK accepts only string values
// with the types specified separately via enums
type deserializedPoint struct {
	timeUnixNano string
	table        string
	tags         []string
	tagKeys      []string
	fields       []*string
}
