package esdb

import (
	"context"
	"encoding/json"
	"errors"
	
	"go-sjy-esdb/es"

	"github.com/olivere/elastic/v7"
)

// CreateDoc 单条添加
func CreateDoc(index, id string, body interface{}) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}
	_, err = db.Index().Index(index).Id(id).BodyJson(body).Do(context.Background())
	return err
}

// UpdateDoc 单条更新
func UpdateDoc(index, id string, body interface{}) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	_, err = db.Update().Index(index).Id(id).Doc(body).Do(context.Background())
	return err
}

// DeleteDoc 删除文档
func DeleteDoc(index, id string) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}
	_, err = db.Delete().Index(index).Id(id).Do(context.Background())
	return err
}

// CreateBulkDoc 批量添加: ids 和 body 的顺序要一一对应
func CreateBulkDoc(index string, ids []string, body []interface{}) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	if len(ids) != len(body) {
		return errors.New("id does not correspond to the number of body")
	}

	bulkRequest := db.Bulk()
	for k, v := range body {
		doc := elastic.NewBulkIndexRequest().Index(index).Doc(v).Id(ids[k])
		bulkRequest = bulkRequest.Add(doc)
	}

	_, err = bulkRequest.Do(context.Background())
	return err
}

// UpdateBulkDoc 批量更新: ids 和 body 的顺序要一一对应
func UpdateBulkDoc(index string, ids []string, body []interface{}) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	if len(ids) != len(body) {
		return errors.New("id does not correspond to the number of body")
	}

	bulkRequest := db.Bulk()
	for k, v := range body {
		doc := elastic.NewBulkUpdateRequest().Index(index).Id(ids[k]).Doc(v).DocAsUpsert(true)
		bulkRequest = bulkRequest.Add(doc)
	}

	_, err = bulkRequest.Do(context.Background())
	return err
}

// DeleteBulkDoc 批量删除
func DeleteBulkDoc(index string, ids []string) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	bulkRequest := db.Bulk()
	for _, id := range ids {
		req := elastic.NewBulkDeleteRequest().Index(index).Id(id)
		bulkRequest = bulkRequest.Add(req)
	}
	_, err = bulkRequest.Do(context.Background())
	return err
}

// FirstDoc 通过 id 取出数据
func FirstDoc(dest interface{}, index, id string) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	res, err := db.Get().Index(index).Id(id).Do(context.Background())
	if err != nil {
		return err
	}
	err = data2Resp(res.Source, dest)
	if err != nil {
		return err
	}

	return nil
}

// GetDocIn 通过ID批量获取数据
func GetDocIn(dest interface{}, index string, ids []string) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}
	mGet := db.MultiGet()
	for _, v := range ids {
		mGet = mGet.Add(elastic.NewMultiGetItem().Index(index).Id(v))
	}
	res, err := mGet.Do(context.Background())
	if err != nil {
		return err
	}

	//遍历取出数据
	var source []json.RawMessage
	for _, v := range res.Docs {
		source = append(source, v.Source)
	}

	j, err := json.Marshal(source)
	if err != nil {
		return err
	}

	err = data2Resp(j, dest)
	if err != nil {
		return err
	}

	return nil

}

type Sort struct {
	Name  string
	IsAsc bool //true-升序 false-倒序
}

// GetDocQuery 根据条件查询
func GetDocQuery(dest interface{}, index string, sort *Sort, offset uint, limit uint, query elastic.Query) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	res, err := db.Search().Index(index).Query(query).Sort(sort.Name, sort.IsAsc).From(int(offset)).Size(int(limit)).Do(context.Background())
	if err != nil {
		return err
	}

	//遍历取出数据
	var source []json.RawMessage
	for _, v := range res.Hits.Hits {
		source = append(source, v.Source)
	}

	j, err := json.Marshal(source)
	if err != nil {
		return err
	}

	err = data2Resp(j, dest)
	if err != nil {
		return err
	}

	return nil
}
