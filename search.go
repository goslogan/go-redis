package redis

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	noSlop                   = -100 // impossible value for slop to indicate none set
	DefaultOffset            = 0    // default first value for return offset
	DefaultLimit             = 10   // default number of results to return
	noLimit                  = 0
	defaultSumarizeSeparator = "..."
	defaultSummarizeLen      = 20
	defaultSummarizeFrags    = 3
	GeoMiles                 = "mi"
	GeoFeet                  = "f"
	GeoKilimetres            = "km"
	GeoMetres                = "m"
	SortAsc                  = "ASC"
	SortDesc                 = "DESC"
	SortNone                 = "" // SortNone is used to indicate that no sorting is required if you want be explicit
	defaultDialect           = 2
)

type SearchCmdAble interface {
	//FTSearch(ctx context.Context, index string, query string, options *QueryOptions) *QueryCmd
	//FTAggregate(ctx context.Context, index string, query string, options *AggregateOptions) *QueryCmd
	FTDropIndex(ctx context.Context, index string, dropDocuments bool) *BoolCmd
	FTCreateIndex(ctx context.Context, index string)
	//FTConfigGet(ctx context.Context, keys ...string) *FTConfigGetCmd
	FTConfigSet(ctx context.Context, name, value string) *BoolCmd
	FTTagVals(ctx context.Context, index, tag string) *StringSliceCmd
	FTList(ctx context.Context) *StringSliceCmd
	// FTInfo(ctx context.Context, index string) *InfoCmd
	FTDictAdd(ctx context.Context, dictionary string, terms ...string) *IntCmd
	FTDictDel(ctx context.Context, dictionary string, terms ...string) *IntCmd
	FTDictDump(ctx context.Context, dictionary string) *StringSliceCmd
	FTSynUpdate(ctx context.Context, index string, group string, terms ...string) *BoolCmd
	//FTSynDump(ctx context.Context, index string) *SynonymDumpCmd
	FTAliasAdd(ctx context.Context, alias, index string) *BoolCmd
	FTAliasDel(ctx context.Context, alias string) *BoolCmd
	FTAliasUpdate(ctx context.Context, alias, index string) *BoolCmd
}

//------------------------------------------------------------------------------

// FTCreate creates a new index.
func (c cmdable) FTCreate(ctx context.Context, index string, options *IndexOptions) *BoolCmd {
	args := []interface{}{"FT.CREATE", index}
	args = append(args, options.serialize()...)
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTDropIndex removes an index, optionally dropping documents in the index.
func (c cmdable) FTDropIndex(ctx context.Context, index string, dropDocuments bool) *BoolCmd {
	args := []interface{}{"FT.DROPINDEX", index}
	if dropDocuments {
		args = append(args, "DD")
	}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

// SearchIndex defines an index to be created with FT.CREATE
// For more information, see [https://redis.io/commands/ft.create/]
type IndexOptions struct {
	On              string   // JSON or HASH
	Prefix          []string // Array of key prefixes
	Filter          string
	Language        string
	LanguageField   string
	Score           float64
	ScoreField      string
	MaxTextFields   bool
	NoOffsets       bool
	Temporary       uint64 // If this is a temporary index, number of seconds until expiry
	NoHighlight     bool
	NoFields        bool
	NoFreqs         bool
	StopWords       []string
	UseStopWords    bool
	SkipInitialscan bool
	Schema          []SchemaAttribute
}

// TagAttribute defines a tag attribute for search index creation
type TagAttribute struct {
	Name           string
	Alias          string
	Sortable       bool
	UnNormalized   bool
	Separator      string
	CaseSensitive  bool
	WithSuffixTrie bool
	NoIndex        bool
}

type TextAttribute struct {
	Name           string
	Alias          string
	Sortable       bool
	UnNormalized   bool
	Phonetic       string
	Weight         float32
	NoStem         bool
	WithSuffixTrie bool
	NoIndex        bool
}

type NumericAttribute struct {
	Name     string
	Alias    string
	Sortable bool
	NoIndex  bool
}

type GeoAttribute struct {
	Name     string
	Alias    string
	Sortable bool
	NoIndex  bool
}

type VectorAttribute struct {
	Name           string
	Alias          string
	Algorithm      string
	Type           string
	Dim            uint64
	DistanceMetric string
	InitialCap     uint64
	BlockSize      uint64
	M              uint64
	EFConstruction uint64
	EFRuntime      uint64
	Epsilon        float64
}

type GeometryAttribute struct {
	Name  string
	Alias string
}

type SchemaAttribute interface {
	serialize() []interface{}
	// parse(key string, value interface{})
}

// NewIndexOptions returns an initialised IndexOptions struct with defaults set
func NewIndexOptions() *IndexOptions {
	return &IndexOptions{
		On:    "hash", // Default
		Score: 1,      // Default
	}
}

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

func NewQueryHighlight() *QueryHighlight {
	return &QueryHighlight{}
}

//------------------------------------------------------------------------------

func (i *IndexOptions) serialize() []interface{} {

	args := []interface{}{"ON", strings.ToUpper(i.On)}
	args = append(args, serializeCountedArgs("PREFIX", false, i.Prefix)...)

	if i.Filter != "" {
		args = append(args, "FILTER", i.Filter)
	}

	if i.Language != "" {
		args = append(args, "LANGUAGE", i.Language)
	}

	if i.LanguageField != "" {
		args = append(args, "LANGUAGE_FIELD", i.LanguageField)
	}

	args = append(args, "SCORE", i.Score)

	if i.ScoreField != "" {
		args = append(args, "SCORE_FIELD", i.ScoreField)
	}

	if i.MaxTextFields {
		args = append(args, "MAXTEXTFIELDS")
	}

	if i.NoOffsets {
		args = append(args, "NOOFFSETS")
	}

	if i.Temporary > 0 {
		args = append(args, "TEMPORARY", i.Temporary)
	}

	if i.NoHighlight && !i.NoOffsets {
		args = append(args, "NOHL")
	}

	if i.NoFields {
		args = append(args, "NOFIELDS")
	}

	if i.NoFreqs {
		args = append(args, "NOFREQS")
	}

	if i.UseStopWords {
		args = append(args, serializeCountedArgs("STOPWORDS", true, i.StopWords)...)
	}

	if i.SkipInitialscan {
		args = append(args, "SKIPINITIALSCAN")
	}

	schema := []interface{}{"SCHEMA"}

	for _, attrib := range i.Schema {
		schema = append(schema, attrib.serialize()...)
	}

	return append(args, schema...)
}

func (a *NumericAttribute) serialize() []interface{} {

	attribs := []interface{}{a.Name}
	if a.Alias != "" {
		attribs = append(attribs, "AS", a.Alias)
	}
	attribs = append(attribs, "NUMERIC")

	if a.Sortable {
		attribs = append(attribs, "SORTABLE")
	}

	if a.NoIndex {
		attribs = append(attribs, "NOINDEX")
	}

	return attribs
}

func (a *TagAttribute) serialize() []interface{} {

	attribs := []interface{}{a.Name}
	if a.Alias != "" {
		attribs = append(attribs, "AS", a.Alias)
	}
	attribs = append(attribs, "TAG")

	if a.Separator != "" {
		attribs = append(attribs, "SEPARATOR", a.Separator)
	}

	if a.Sortable {
		attribs = append(attribs, "SORTABLE")
		if a.UnNormalized {
			attribs = append(attribs, "UNF")
		}
	}

	if a.CaseSensitive {
		attribs = append(attribs, "CASESENSITIVE")
	}
	if a.NoIndex {
		attribs = append(attribs, "NOINDEX")
	}

	return attribs
}

func (a *TextAttribute) serialize() []interface{} {

	attribs := []interface{}{a.Name}
	if a.Alias != "" {
		attribs = append(attribs, "AS", a.Alias)
	}

	attribs = append(attribs, "TEXT")

	if a.Weight != 0 {
		attribs = append(attribs, "WEIGHT", a.Weight)
	}

	if a.Sortable {
		attribs = append(attribs, "SORTABLE")
		if a.UnNormalized {
			attribs = append(attribs, "UNF")
		}
	}
	if a.Phonetic != "" {
		attribs = append(attribs, "PHONETIC", a.Phonetic)
	}
	if a.NoStem {
		attribs = append(attribs, "NOSTEM")
	}

	if a.NoIndex {
		attribs = append(attribs, "NOINDEX")
	}

	return attribs
}

func (a *GeometryAttribute) serialize() []interface{} {
	attribs := []interface{}{a.Name}
	if a.Alias != "" {
		attribs = append(attribs, "AS", a.Alias)
	}
	attribs = append(attribs, "GEOMETRY")

	return attribs
}

func (a *GeoAttribute) serialize() []interface{} {
	attribs := []interface{}{a.Name}
	if a.Alias != "" {
		attribs = append(attribs, "AS", a.Alias)
	}

	attribs = append(attribs, "GEO")

	if a.Sortable {
		attribs = append(attribs, "SORTABLE")
	}

	if a.NoIndex {
		attribs = append(attribs, "NOINDEX")
	}
	return attribs
}

func (a *VectorAttribute) serialize() []interface{} {
	attribs := []interface{}{a.Name}
	if a.Alias != "" {
		attribs = append(attribs, "AS", a.Alias)
	}

	attribs = append(attribs, "VECTOR")
	attribs = append(attribs, a.Algorithm)

	params := []interface{}{"TYPE", a.Type, "DIM", a.Dim, "DISTANCE_METRIC", a.DistanceMetric}
	if a.InitialCap != 0 {
		params = append(params, "INITIAL_CAP", a.InitialCap)
	}
	if strings.ToLower(a.Algorithm) == "FLAT" && a.BlockSize != 0 {
		params = append(params, "BLOCK_SIZE", a.BlockSize)
	}
	if strings.ToLower(a.Algorithm) == "HNSW" {
		if a.M != 0 {
			params = append(params, "M", a.M)
		}
		if a.EFConstruction != 0 {
			params = append(params, "EF_CONSTRUCTION", a.EFConstruction)
		}
		if a.EFRuntime != 0 {
			params = append(params, "EF_RUNTIME", a.EFRuntime)
		}
		if a.Epsilon != 0 {
			params = append(params, "EPSILON", a.Epsilon)
		}
	}
	attribs = append(attribs, len(params))
	attribs = append(attribs, params...)

	return attribs
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
				fields = append(fields, ret.Name, "as", ret.As)
			}
		}
		return append([]interface{}{"return", len(fields)}, fields...)
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

// serialize prepares the summarisation to be passed to Redis.
func (s *QuerySummarize) serialize() []interface{} {
	args := []interface{}{"SUMMARIZE"}
	args = append(args, serializeCountedArgs("fields", false, s.Fields)...)
	args = append(args, "frags", s.Frags)
	args = append(args, "len", s.Len)
	args = append(args, "separator", s.Separator)
	return args
}

// serialize prepares the highlighting to be passed to Redis.
func (h *QueryHighlight) serialize() []interface{} {
	args := []interface{}{"HIGHLIGHT"}
	args = append(args, serializeCountedArgs("fields", false, h.Fields)...)
	if h.OpenTag != "" || h.CloseTag != "" {
		args = append(args, "tags", h.OpenTag, h.CloseTag)
	}
	return args
}

// Serialize the limit for output in an FT.SEARCH
func (ql *QueryLimit) serialize() []interface{} {
	if ql.Offset == DefaultOffset && ql.Num == DefaultLimit {
		return nil
	} else {
		return []interface{}{"limit", ql.Offset, ql.Num}
	}
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

//------------------------------------------------------------------------------on

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
