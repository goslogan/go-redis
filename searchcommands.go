package redis

import (
	"context"
)

// FTDropIndex removes an index, optionally dropping documents in the index.
func (c cmdable) FTDropIndex(ctx context.Context, index string, dropDocuments bool) *BoolCmd {
	args := []interface{}{"ft.dropindex", index}
	if dropDocuments {
		args = append(args, "DD")
	}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTCreate creates a new index.
func (c cmdable) FTCreate(ctx context.Context, index string, options *IndexOptions) *BoolCmd {
	args := []interface{}{"ft.create", index}
	args = append(args, options.serialize()...)
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTAggregate runs a search query on an index, and perform saggregate transformations on the results, extracting statistics etc from them
func (c cmdable) FTAggregate(ctx context.Context, index, query string, options *AggregateOptions) *AggregateCmd {
	args := []interface{}{"ft.aggregate", index, query}
	args = append(args, options.serialize()...)
	cmd := NewAggregateCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTSearch queries an index (on hashes)
func (c cmdable) FTSearch(ctx context.Context, index string, query string, qryOptions *QueryOptions) *QueryCmd {
	args := []interface{}{"ft.search", index, query}
	if qryOptions == nil {
		qryOptions = NewQueryOptions()
	}
	args = append(args, qryOptions.serialize()...)

	cmd := NewQueryCmd(ctx, c, args...)
	cmd.options = qryOptions

	_ = c(ctx, cmd)
	return cmd
}

// FTSearch queries an index on JSON documents
func (c cmdable) FTSearchJSON(ctx context.Context, index string, query string, qryOptions *QueryOptions) *QueryCmd {
	args := []interface{}{"ft.search", index, query}
	if qryOptions == nil {
		qryOptions = NewQueryOptions()
	}
	qryOptions.json = true
	args = append(args, qryOptions.serialize()...)

	cmd := NewQueryCmd(ctx, c, args...)
	cmd.options = qryOptions

	_ = c(ctx, cmd)
	return cmd
}

// FTConfigGet retrieves public config info from the search config
func (c cmdable) FTConfigGet(ctx context.Context, keys ...string) *ConfigGetCmd {
	args := make([]interface{}, len(keys)+2)
	args[0] = "ft.config"
	args[1] = "get"
	for n, arg := range keys {
		args[n+2] = arg
	}

	if len(keys) == 0 {
		args = append(args, "*")
	}

	cmd := NewConfigGetCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTConfigGet sets values in the search config
func (c cmdable) FTConfigSet(ctx context.Context, name, value string) *BoolCmd {
	args := []interface{}{"ft.config", "set", name, value}

	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTTagVals returns the distinct values for a given tag
func (c cmdable) FTTagVals(ctx context.Context, index, tag string) *StringSliceCmd {
	args := []interface{}{"ft.tagvals", index, tag}

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTList returns a list of all the indexes currently defined
func (c cmdable) FTList(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx)
	_ = c(ctx, cmd)
	return cmd
}

/*******************************************************************************
*
* DICTIONARIES
*
*******************************************************************************/

// FTDictAdd adds one more terms to a dictionary
func (c cmdable) FTDictAdd(ctx context.Context, dictionary string, terms ...string) *IntCmd {

	args := make([]interface{}, len(terms)+2)
	args[0] = "ft.dictadd"
	args[1] = dictionary
	for n, term := range terms {
		args[n+2] = term
	}

	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)

	return cmd

}

// FTDictDel removes terms from a dictionary
func (c cmdable) FTDictDel(ctx context.Context, dictionary string, terms ...string) *IntCmd {

	args := make([]interface{}, len(terms)+2)
	args[0] = "ft.dictdel"
	args[1] = dictionary
	for n, term := range terms {
		args[n+2] = term
	}

	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)

	return cmd
}

// FTDictDump returns a slice containing all the terms in a dictionary
func (c cmdable) FTDictDump(ctx context.Context, dictionary string) *StringSliceCmd {

	args := []interface{}{"ft.dictdump", dictionary}

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)

	return cmd
}

/*******************************************************************************
*
* SYNONYMS
*
*******************************************************************************/

// FTSynUpdate adds to or modifies a synonym group
func (c cmdable) FTSynUpdate(ctx context.Context, index string, group string, terms ...string) *BoolCmd {
	args := make([]interface{}, len(terms)+3)
	args[0] = "ft.synupdate"
	args[1] = index
	args[2] = group
	for n, term := range terms {
		args[n+2] = term
	}

	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)

	return cmd
}

// FTSynDump returns the contents of synonym map for an index
func (c cmdable) FTSynDump(ctx context.Context, index string) *SynonymDumpCmd {
	args := []interface{}{"ft.syndump", index}
	cmd := NewSynonymDumpCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

/*******************************************************************************
*
* ALIASES
*
*******************************************************************************/

// FTAliasAdd add an alias to an index.
func (c cmdable) FTAliasAdd(ctx context.Context, alias, index string) *BoolCmd {
	args := []interface{}{"ft.aliasadd", alias, index}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTAliasDel deletes an alias
func (c cmdable) FTAliasDel(ctx context.Context, alias string) *BoolCmd {
	args := []interface{}{"ft.aliasdel", alias}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTAliasDel deletes an alias
func (c cmdable) FTAliasUpdate(ctx context.Context, alias, index string) *BoolCmd {
	args := []interface{}{"ft.aliasupdate", alias, index}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
