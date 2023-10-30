package redis

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		Limit:     NewQueryLimit(DefaultOffset, DefaultLimit),
		Slop:      noSlop,
		SortOrder: SortAsc,
		Dialect:   defaultDialect,
		Params:    map[string]interface{}{},
	}
}

func NewQueryLimit(first int, num int) *QueryLimit {
	return &QueryLimit{Offset: first, Num: num}
}

func NewQuerySummarize() *QuerySummarize {
	return &QuerySummarize{}
}

func DefaultQuerySummarize() *QuerySummarize {
	return &QuerySummarize{
		Separator: DefaultSumarizeSeparator,
		Len:       DefaultSummarizeLen,
		Frags:     DefaultSummarizeFrags,
	}
}

func NewQueryHighlight() *QueryHighlight {
	return &QueryHighlight{}
}

//------------------------------------------------------------------------------

type QueryOptions struct {
	NoContent    bool
	Verbatim     bool
	NoStopWords  bool
	WithScores   bool
	WithPayloads bool
	WithSortKeys bool
	InOrder      bool
	ExplainScore bool
	Limit        *QueryLimit
	Return       []QueryReturn
	Filters      []QueryFilter
	InKeys       []string
	InFields     []string
	Language     string
	Slop         int8
	Expander     string
	Scorer       string
	SortBy       string
	SortOrder    string
	Dialect      uint8
	Timeout      time.Duration
	Summarize    *QuerySummarize
	HighLight    *QueryHighlight
	GeoFilters   []GeoFilter
	Params       map[string]interface{}
	json         bool
}

type QuerySummarize struct {
	Fields    []string
	Frags     int32
	Len       int32
	Separator string
}

type QueryHighlight struct {
	Fields   []string
	OpenTag  string
	CloseTag string
}

type QueryReturn struct {
	Name string
	As   string
}

type QueryFilter struct {
	Attribute string
	Min       interface{} // either a numeric value or +inf, -inf or "(" followed by numeric
	Max       interface{} // as above
}

// queryLimit defines the results by offset and number.
type QueryLimit struct {
	Offset int
	Num    int
}

// serialize converts a query struct to a slice of  interface{}
// ready for execution against Redis
func (q *QueryOptions) serialize() []interface{} {
	var args = []interface{}{}

	args = q.appendFlagArg(args, q.NoContent, "NOCONTENT")
	args = q.appendFlagArg(args, q.Verbatim, "VERBATIM")
	args = q.appendFlagArg(args, q.NoStopWords, "NOSTOPWORDS")
	args = q.appendFlagArg(args, q.WithScores, "WITHSCORES")
	args = q.appendFlagArg(args, q.WithPayloads, "WITHPAYLOADS")
	args = q.appendFlagArg(args, q.WithSortKeys, "WITHSORTKEYS")
	args = append(args, q.serializeFilters()...)
	for _, gf := range q.GeoFilters {
		args = append(args, gf.serialize()...)
	}
	args = append(args, q.serializeReturn()...)
	if q.Summarize != nil {
		args = append(args, q.Summarize.serialize()...)
	}
	if q.HighLight != nil {
		args = append(args, q.HighLight.serialize()...)
	}

	if q.Slop != noSlop {
		args = appendStringArg(args, "SLOP", fmt.Sprintf("%d", q.Slop))
	}

	if q.Timeout != 0 {
		args = appendStringArg(args, "TIMEOUT", fmt.Sprintf("%d", q.Timeout.Milliseconds()))
	}
	args = q.appendFlagArg(args, q.InOrder, "INORDER")
	args = appendStringArg(args, "LANGUAGE", q.Language)

	args = append(args, serializeCountedArgs("INKEYS", false, q.InKeys)...)
	args = append(args, serializeCountedArgs("INFIELDS", false, q.InFields)...)

	args = q.appendFlagArg(args, q.ExplainScore && q.WithScores, "EXPLAINSCORE")

	if q.SortBy != "" {
		args = append(args, "SORTBY", q.SortBy)
		if q.SortOrder != "" {
			args = append(args, q.SortOrder)
		}
	}

	if q.Limit != nil {
		args = append(args, q.Limit.serialize()...)
	}

	if len(q.Params) != 0 {
		args = append(args, "PARAMS", len(q.Params))
		for n, v := range q.Params {
			args = append(args, n, v)
		}
	}

	if q.Dialect != defaultDialect {
		args = append(args, "DIALECT", q.Dialect)
	}

	return args
}

// appendFlagArg appends the values to args if flag is true. args is returned
func (q *QueryOptions) appendFlagArg(args []interface{}, flag bool, value string) []interface{} {
	if flag {
		return append(args, value)
	} else {
		return args
	}
}

// appendStringArg appends the name and value if value is not empty
func appendStringArg(args []interface{}, name, value string) []interface{} {
	if value != "" {
		return append(args, name, value)
	} else {
		return args
	}
}

func (q *QueryOptions) serializeReturn() []interface{} {
	if len(q.Return) > 0 {
		fields := []interface{}{}
		for _, ret := range q.Return {
			if ret.As == "" {
				fields = append(fields, ret.Name)
			} else {
				fields = append(fields, ret.Name, "AS", ret.As)
			}
		}
		return append([]interface{}{"RETURN", len(fields)}, fields...)
	} else {
		return nil
	}
}

// serialize the filters
func (q *QueryOptions) serializeFilters() []interface{} {
	args := []interface{}{}
	for _, f := range q.Filters {
		args = append(args, f.serialize()...)
	}
	return args
}

// resultSize uses the query to work out how many entries
// in the query raw results slice are used per result.
func (q *QueryOptions) resultSize() int {
	count := 2 // default to 2 - key and value

	if q.WithScores { // one more if returning scores
		count += 1
	}

	if q.NoContent { // one less if not content
		count -= 1
	}

	return count
}

// serialize prepares the summarisation to be passed to Redis.
func (s *QuerySummarize) serialize() []interface{} {
	args := []interface{}{"SUMMARIZE"}
	args = append(args, serializeCountedArgs("FIELDS", false, s.Fields)...)
	args = append(args, "FRAGS", s.Frags)
	args = append(args, "LEN", s.Len)
	args = append(args, "SEPARATOR", s.Separator)
	return args
}

// serialize prepares the highlighting to be passed to Redis.
func (h *QueryHighlight) serialize() []interface{} {
	args := []interface{}{"HIGHLIGHT"}
	args = append(args, serializeCountedArgs("FIELDS", false, h.Fields)...)
	if h.OpenTag != "" || h.CloseTag != "" {
		args = append(args, "TAGS", h.OpenTag, h.CloseTag)
	}
	return args
}

// Serialize the limit for output in an FT.SEARCH
func (ql *QueryLimit) serialize() []interface{} {
	if ql.Offset == DefaultOffset && ql.Num == DefaultLimit {
		return nil
	} else {
		return []interface{}{"LIMIT", ql.Offset, ql.Num}
	}
}

// NewQueryFilter returns a filter with the min and max properties to set + and - infinity.
func NewQueryFilter(attribute string) QueryFilter {
	qf := QueryFilter{Attribute: attribute}
	qf.Min = FilterValue(math.Inf(-1), false)
	qf.Max = FilterValue(math.Inf(1), false)
	return qf
}

// FilterValue formats a value for use in a filter and returns it
func FilterValue(val float64, exclusive bool) interface{} {
	prefix := ""
	if exclusive {
		prefix = "("
	}

	if math.IsInf(val, -1) {
		return prefix + "-inf"
	} else if math.IsInf(val, 1) {
		return prefix + "+inf"
	} else {
		return fmt.Sprintf("%s%f", prefix, val)
	}
}

// serialize converts a filter list to an array of interface{} objects for execution
func (q *QueryFilter) serialize() []interface{} {
	return []interface{}{"filter", q.Attribute, q.Min, q.Max}
}

/******************************************************************************
* Geofilters
******************************************************************************/

// GeoFilter represents a location and radius to be used in a search query
type GeoFilter struct {
	Attribute         string
	Long, Lat, Radius float64
	Units             string
}

func (gf *GeoFilter) serialize() []interface{} {
	return []interface{}{"geofilter", gf.Attribute, gf.Long, gf.Lat, gf.Radius, gf.Units}
}

//------------------------------------------------------------------------------

type QueryResults []*QueryResult

type ResultValue interface {
	parse([]interface{}) error
}

type QueryResult struct {
	Key         string
	Score       float64
	Explanation []interface{}
	Values      ResultValue
}

type HashQueryValue struct {
	Value map[string]string
}

type JSONQueryValue struct {
	Value    map[string]interface{}
	rawValue map[string]string
}

func (r *HashQueryValue) parse(input []interface{}) error {
	results := make(map[string]string, len(input)/2)
	key := ""
	for i := 0; i < len(input); i += 2 {
		key = input[i].(string)
		value := input[i+1].(string)
		results[key] = value
	}
	r.Value = results
	return nil
}

func (r *HashQueryValue) Scan(dst interface{}) error {
	sCmd := NewMapStringStringResult(r.Value, nil)
	return sCmd.Scan(dst)
}

func (r *JSONQueryValue) parse(input []interface{}) error {

	key := input[0].(string)
	rawValue := input[1].(string)
	var result interface{}
	err := json.Unmarshal([]byte(rawValue), &result)

	if r.Value == nil {
		r.rawValue = make(map[string]string)
		r.Value = make(map[string]interface{})
	}

	r.rawValue[key] = rawValue
	r.Value[key] = result
	return err
}

func (r *JSONQueryValue) Scan(path string, to interface{}) error {
	return json.Unmarshal([]byte(r.rawValue[path]), to)
}

// Key returns the individual result with the
// given key
func (q QueryResults) Key(key string) *QueryResult {
	for _, r := range q {
		if r.Key == key {
			return r
		}
	}
	return nil
}

// Keys returns the redis keys for all of the results
func (q QueryResults) Keys() []string {
	results := make([]string, len(q))
	for i, k := range q {
		results[i] = k.Key
	}

	return results
}

//------------------------------------------------------------------------------

// serializeCountedArgs is used to serialize a string array to
// NAME <count> values. If incZero is true then NAME 0 will be generated
// otherwise empty results will not be generated.
func serializeCountedArgs(name string, incZero bool, args []string) []interface{} {
	if len(args) > 0 || incZero {
		result := make([]interface{}, 2+len(args))

		result[0] = name
		result[1] = len(args)
		for pos, val := range args {
			result[pos+2] = val
		}

		return result
	} else {
		return nil
	}
}
