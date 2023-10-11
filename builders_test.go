package redis_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("We can build query options", Label("builders", "ft.search"), func() {

	It("can construct default query options", func() {
		base := redis.NewQueryOptions()
		built := redis.NewQueryBuilder()

		Expect(base).To(Equal(built.Options()))
	})

	It("can construct options with simple flags set", func() {
		base := redis.NewQueryOptions()
		base.Dialect = 2
		base.ExplainScore = true
		base.NoContent = true
		base.Timeout = time.Duration(1000)
		base.NoStopWords = true
		base.Verbatim = true

		built := redis.NewQueryBuilder().
			Dialect(2).
			ExplainScore().
			NoContent().
			Timeout(time.Duration(1000)).
			Verbatim().
			NoStopWords()

		Expect(base).To(Equal(built.Options()))

	})

	It("can construct queries with parameters", func() {
		base := redis.NewQueryOptions()
		base.Params = map[string]interface{}{
			"foo": "one",
			"bar": 2,
		}

		built := redis.NewQueryBuilder().
			Param("foo", "one").
			Param("bar", 2)

		Expect(base).To(Equal(built.Options()))
	})

	It("can construct queries with geofilters", func() {
		base := redis.NewQueryOptions()
		base.GeoFilters = []redis.GeoFilter{
			{
				Attribute: "test",
				Long:      100,
				Lat:       200,
				Radius:    300,
				Units:     "m",
			},
		}
		built := redis.NewQueryBuilder().
			GeoFilter("test", 100, 200, 300, "m")

		Expect(base).To(Equal(built.Options()))
	})

	It("can construct queries with filters", func() {
		base := redis.NewQueryOptions()
		base.Filters = []redis.QueryFilter{
			{
				Attribute: "test",
				Min:       -100,
				Max:       "+inf",
			},
		}
		built := redis.NewQueryBuilder().
			Filter("test", -100, "+inf")

		Expect(base).To(Equal(built.Options()))
	})

})

var _ = Describe("We can build aggregate options", Label("builders", "ft.aggregate"), func() {

	It("can construct default query options", func() {
		base := redis.NewAggregateOptions()
		built := redis.NewAggregateBuilder()

		Expect(base).To(Equal(built.Options()))
	})

	It("can construct options with simple flags set", func() {
		base := redis.NewAggregateOptions()
		base.Dialect = 2
		base.Steps = append(base.Steps, redis.AggregateFilter("@test != 3"))
		base.Timeout = time.Duration(1000)
		base.Verbatim = true

		built := redis.NewAggregateBuilder().
			Dialect(2).
			Timeout(time.Duration(1000)).
			Verbatim().
			Filter("@test != 3")

		Expect(base).To(Equal(built.Options()))

	})

	It("can construct queries with parameters", func() {
		base := redis.NewAggregateOptions()
		base.Params = map[string]interface{}{
			"foo": "one",
			"bar": 2,
		}

		built := redis.NewAggregateBuilder().
			Param("foo", "one").
			Param("bar", 2)

		Expect(base).To(Equal(built.Options()))
	})

	It("can construct a single group by", func() {
		base := redis.NewAggregateOptions()
		base.Steps = append(base.Steps, &redis.AggregateGroupBy{
			Properties: []string{"@name"},
			Reducers: []redis.AggregateReducer{
				{
					Name: "count",
					As:   "nameCount",
				},
			},
		})

		built := redis.NewAggregateBuilder().
			GroupBy(redis.NewGroupByBuilder().
				Properties([]string{"@name"}).
				Reduce(redis.ReduceCount("nameCount")).
				GroupBy())

		Expect(base).To(Equal(built.Options()))
	})

	It("can build a complex aggregate", func() {
		base := redis.NewAggregateOptions()
		base.Steps = append(base.Steps, &redis.AggregateApply{
			Expression: "@timestamp - (@timestamp % 86400)",
			As:         "day",
		})
		base.Steps = append(base.Steps, &redis.AggregateGroupBy{
			Properties: []string{"@day", "@country"},
			Reducers: []redis.AggregateReducer{{
				Name: "count",
				As:   "num_visits",
			}}})

		base.Steps = append(base.Steps, &redis.AggregateSort{
			Keys: []redis.AggregateSortKey{{
				Name:  "@day",
				Order: redis.SortAsc,
			}, {
				Name:  "@country",
				Order: redis.SortDesc,
			}},
		})

		built := redis.NewAggregateBuilder().
			Apply("@timestamp - (@timestamp % 86400)", "day").
			GroupBy(redis.NewGroupByBuilder().
				Properties([]string{"@day", "@country"}).
				Reduce(redis.ReduceCount("num_visits")).
				GroupBy()).
			SortBy([]redis.AggregateSortKey{{Name: "@day", Order: redis.SortAsc}, {Name: "@country", Order: redis.SortDesc}})

		Expect(base).To(Equal(built.Options()))

	})

})
