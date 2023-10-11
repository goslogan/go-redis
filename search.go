// redis main module - defines the client class
package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/proto"
)

type SearchCmdAble interface {
	FTSearch(ctx context.Context, index string, query string, options *QueryOptions) *QueryCmd
	FTAggregate(ctx context.Context, index string, query string, options *AggregateOptions) *QueryCmd
	FTDropIndex(ctx context.Context, index string, dropDocuments bool) *BoolCmd
	FTCreateIndex(ctx context.Context, index string)
	FTConfigGet(ctx context.Context, keys ...string) *FTConfigGetCmd
	FTConfigSet(ctx context.Context, name, value string) *BoolCmd
	FTTagVals(ctx context.Context, index, tag string) *StringSliceCmd
	FTList(ctx context.Context) *StringSliceCmd
	// FTInfo(ctx context.Context, index string) *InfoCmd
	FTDictAdd(ctx context.Context, dictionary string, terms ...string) *IntCmd
	FTDictDel(ctx context.Context, dictionary string, terms ...string) *IntCmd
	FTDictDump(ctx context.Context, dictionary string) *StringSliceCmd
	FTSynUpdate(ctx context.Context, index string, group string, terms ...string) *BoolCmd
	FTSynDump(ctx context.Context, index string) *SynonymDumpCmd
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

/*******************************************************************************
 ***** FTConfigGetCmd 													  ******
 *******************************************************************************/

type FTConfigGetCmd struct {
	baseCmd
	val map[string]string
}

func NewFTConfigGetCmd(ctx context.Context, args ...interface{}) *FTConfigGetCmd {
	return &FTConfigGetCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *FTConfigGetCmd) readReply(rd *proto.Reader) error {

}

func (c *FTConfigGetCmd) postProcess() error {
	if result, err := c.Slice(); err == nil {
		configs := make(map[string]string, len(result))
		for _, cfg := range result {
			key := cfg.([]interface{})[0].(string)
			if key[0] != '_' {
				if cfg.([]interface{})[1] != nil {
					val := cfg.([]interface{})[1].(string)
					configs[key] = val
				} else {
					configs[key] = ""
				}
			}

		}
		c.SetVal(configs)
	}
	return nil
}

func (cmd *FTConfigGetCmd) SetVal(val map[string]string) {
	cmd.val = val
}

func (cmd *FTConfigGetCmd) Val() map[string]string {
	return cmd.val
}

func (cmd *FTConfigGetCmd) Result() (map[string]string, error) {
	return cmd.Val(), cmd.Err()
}

/*******************************************************************************
*
* SynDumpCmd
*
*******************************************************************************/

type SynonymDumpCmd struct {
	*Cmd
	val map[string][]string
}

var _ Cmder = (*SynonymDumpCmd)(nil)

func NewSynonymDumpCmd(ctx context.Context, args ...interface{}) *SynonymDumpCmd {
	return &SynonymDumpCmd{
		Cmd: NewCmd(ctx, args...),
	}
}

func (c *SynonymDumpCmd) postProcess() error {
	if result, err := c.Slice(); err == nil {
		synonymMap := make(map[string][]string)
		for n := 0; n < len(result); n += 2 {
			synonym := result[n].(string)
			groups := make([]string, len(result[n+1].([]interface{})))
			for m, group := range result[n+1].([]interface{}) {
				groups[m] = group.(string)
			}
			synonymMap[synonym] = groups

		}
		c.SetVal(synonymMap)
	}
	return nil
}

func (cmd *SynonymDumpCmd) SetVal(val map[string][]string) {
	cmd.val = val
}

func (cmd *SynonymDumpCmd) Val() map[string][]string {
	return cmd.val
}

func (cmd *SynonymDumpCmd) Result() (map[string][]string, error) {
	return cmd.Val(), cmd.Err()
}

/*******************************************************************************
*
* InfoCmd
*
*******************************************************************************/

/*******************************************************************************
*
* IntSlicePointerCmd
* used to represent a RedisJSON response where the result is either an integer or nil
*
*******************************************************************************/

type IntSlicePointerCmd struct {
	*SliceCmd
	val []*int64
}

// NewIntSlicePointerCmd initialises an IntSlicePointerCmd
func NewIntSlicePointerCmd(ctx context.Context, args ...interface{}) *IntSlicePointerCmd {
	return &IntSlicePointerCmd{
		SliceCmd: NewSliceCmd(ctx, args...),
	}
}

// postProcess converts an array of bulk string responses into
// an array of arrays of interfaces.
// an array of json.RawMessage objects
func (c *IntSlicePointerCmd) postProcess() error {

	if len(c.SliceCmd.Val()) == 0 {
		c.val = nil
		c.SetErr(nil)
		return nil
	}

	results := []*int64{}

	for _, val := range c.SliceCmd.Val() {
		var result int64
		if val == nil {
			results = append(results, nil)
		} else {
			result = val.(int64)
			results = append(results, &result)
		}
	}

	c.SetVal(results)
	return nil
}

func (cmd *IntSlicePointerCmd) SetVal(val []*int64) {
	cmd.val = val
}

func (cmd *IntSlicePointerCmd) Val() []*int64 {
	return cmd.val
}

func (cmd *IntSlicePointerCmd) Result() ([]*int64, error) {
	return cmd.Val(), cmd.Err()
}

/*******************************************************************************
*
* AggregateCmd
* used to manage the results from FT.AGGREGATE calls
*
*******************************************************************************/

type AggregateCmd struct {
	*SliceCmd
	val []map[string]string
}

func NewAggregateCmd(ctx context.Context, args ...interface{}) *AggregateCmd {
	return &AggregateCmd{
		SliceCmd: NewSliceCmd(ctx, args...),
	}
}

func (c *AggregateCmd) postProcess() error {
	if len(c.SliceCmd.Val()) == 0 {
		c.val = nil
		c.SetErr(nil)
		return nil
	}

	results := make([]map[string]string, len(c.SliceCmd.Val())-1)

	for n, entry := range c.SliceCmd.Val() {

		if n > 0 {
			row := entry.([]interface{})
			asStrings := map[string]string{}
			for m := 0; m < len(row); m += 2 {
				asStrings[row[m].(string)] = row[m+1].(string)
			}
			results[n-1] = asStrings
		}
	}

	c.SetVal(results)
	return nil
}

func (cmd *AggregateCmd) SetVal(val []map[string]string) {
	cmd.val = val
}

func (cmd *AggregateCmd) Val() []map[string]string {
	return cmd.val
}

func (cmd *AggregateCmd) Result() ([]map[string]string, error) {
	return cmd.Val(), cmd.Err()
}
