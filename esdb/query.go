package esdb

import "github.com/olivere/elastic/v7"

// TermsQuery 精准匹配
func TermsQuery(name string, values []interface{}) *elastic.TermsQuery {
	return elastic.NewTermsQuery(name, values...)
}

// MatchAllQuery 全部匹配
func MatchAllQuery() *elastic.MatchAllQuery {
	return elastic.NewMatchAllQuery()
}

// MatchQuery 分词匹配
func MatchQuery(name string, value interface{}) *elastic.MatchQuery {
	return elastic.NewMatchQuery(name, value)
}

// MatchPhraseQuery 短语匹配
func MatchPhraseQuery(name string, value interface{}) *elastic.MatchPhraseQuery {
	return elastic.NewMatchPhraseQuery(name, value)
}

// MatchPhrasePrefixQuery 前缀匹配
func MatchPhrasePrefixQuery(name string, value interface{}) *elastic.MatchPhrasePrefixQuery {
	return elastic.NewMatchPhrasePrefixQuery(name, value)
}

// RangeQuery 范围匹配
func RangeQuery(name string) *elastic.RangeQuery {
	return elastic.NewRangeQuery(name)
}

// BoolQuery 组合查询
func BoolQuery(query ...elastic.Query) *elastic.BoolQuery {
	boolQuery := elastic.NewBoolQuery()
	for _, v := range query {
		boolQuery = boolQuery.Must(v)
	}
	return boolQuery
}

// BoolNotQuery not查询
func BoolNotQuery(query ...elastic.Query) *elastic.BoolQuery {
	boolQuery := elastic.NewBoolQuery()
	for _, v := range query {
		boolQuery = boolQuery.MustNot(v)
	}
	return boolQuery
}

// BoolShouldQuery or查询
func BoolShouldQuery(query ...elastic.Query) *elastic.BoolQuery {
	boolQuery := elastic.NewBoolQuery()
	for _, v := range query {
		boolQuery = boolQuery.Should(v)
	}
	return boolQuery
}
