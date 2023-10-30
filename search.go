package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/proto"
)

const (
	noSlop                   = -100 // impossible value for slop to indicate none set
	DefaultOffset            = 0    // default first value for return offset
	DefaultLimit             = 10   // default number of results to return
	noLimit                  = 0
	DefaultSumarizeSeparator = "..."
	DefaultSummarizeLen      = 20
	DefaultSummarizeFrags    = 3
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

type QueryCmd struct {
	baseCmd
	val     QueryResults
	options *QueryOptions
	process cmdable // used to initialise iterator
	count   int64   // contains the total number of results if the query was successful
}

// NewQueryCmd returns an initialised query command.
func NewQueryCmd(ctx context.Context, process cmdable, args ...interface{}) *QueryCmd {
	return &QueryCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *QueryCmd) SetVal(val QueryResults) {
	cmd.val = val
}

func (cmd *QueryCmd) Val() QueryResults {
	return cmd.val
}

func (cmd *QueryCmd) Result() (QueryResults, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *QueryCmd) Len() int {
	if cmd.Err() != nil {
		return 0
	} else {
		return len(cmd.val)
	}
}

func (cmd *QueryCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *QueryCmd) SetCount(count int64) {
	cmd.count = count
}

// Count returns the total number of results from a successful query.
func (cmd *QueryCmd) Count() int64 {
	return cmd.count
}

// Iterator returns an iterator for the search.
func (cmd *QueryCmd) Iterator(ctx context.Context) *SearchIterator {
	return NewSearchIterator(ctx, cmd, cmd.process)
}

func (cmd *QueryCmd) readReply(rd *proto.Reader) error {
	rawResults, err := rd.ReadSlice()

	if err != nil {
		return err
	}

	resultSize := cmd.options.resultSize()
	resultCount := rawResults[0].(int64)
	results := make([]*QueryResult, 0)

	for i := 1; i < len(rawResults); i += resultSize {
		j := 0
		var score float64 = 0
		var explanation []interface{}

		key := rawResults[i+j].(string)
		j++

		if cmd.options.WithScores {
			if cmd.options.ExplainScore {
				scoreData := rawResults[i+j].([]interface{})
				score = scoreData[0].(float64)
				explanation = scoreData[1].([]interface{})

			} else {
				score, _ = rawResults[i+j].(float64)
			}
			j++
		}

		result := QueryResult{
			Key:         key,
			Score:       score,
			Explanation: explanation,
			Values:      nil,
		}

		if !cmd.options.NoContent {

			if cmd.options.json {
				result.Values = &JSONQueryValue{}
			} else {
				result.Values = &HashQueryValue{}
			}

			if err := result.Values.parse(rawResults[i+j].([]interface{})); err != nil {
				return err
			}
		}

		results = append(results, &result)
		j++

	}

	cmd.SetCount(resultCount)
	cmd.SetVal(QueryResults(results))
	return nil
}
