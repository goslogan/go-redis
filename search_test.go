package redis_test

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("JSON Commands", Label("search"), func() {

	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{Addr: ":6379"})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("FT.CREATE", Label("ft.create"), func() {

		It("can FT.CREATE the simplest JSON index", func() {
			createCmd := client.FTCreate(ctx, "jsimple", redis.NewIndexBuilder().Schema(&redis.TextAttribute{
				Name:  "$.foo",
				Alias: "bar",
			}).On("json").Options())
			Expect(createCmd.Err()).NotTo(HaveOccurred())
			Expect(createCmd.String()).To(Equal("FT.CREATE jsimple ON JSON SCORE 1 SCHEMA $.foo AS bar TEXT: true"))
		})

		It("can FT.CREATE a more complex index", func() {
			createCmd := client.FTCreate(ctx, "jwithoptions", redis.NewIndexBuilder().
				Prefix("jaccount:").
				On("json").
				MaxTextFields().
				Score(0.5).
				Language("spanish").
				Schema(&redis.TextAttribute{
					Name:  "$.foo",
					Alias: "bar",
				}).Options())
			Expect(createCmd.Err()).NotTo(HaveOccurred())
			Expect(createCmd.String()).To(Equal("FT.CREATE jwithoptions ON JSON PREFIX 1 jaccount: LANGUAGE spanish SCORE 0.5 MAXTEXTFIELDS SCHEMA $.foo AS bar TEXT: true"))
		})

	})

	Describe("FT.DROPINDEX", Label("ft.dropindex"), func() {

		// TODO: think about dropping an index with documentation deletion on

		It("can FT.DROP", func() {
			createCmd := client.FTCreate(ctx, "droptest", redis.NewIndexBuilder().Schema(&redis.TextAttribute{
				Name:  "$.foo",
				Alias: "bar",
			}).On("json").Options())
			Expect(createCmd.Err()).NotTo(HaveOccurred())
			res, err := client.FTDropIndex(ctx, "droptest", false).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(BeTrue())

		})
	})

	Describe("We can build query options", Label("builders", "search"), func() {

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

})
