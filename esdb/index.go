package esdb

import (
	"context"
	"errors"
	"fmt"

	"go-sjy-esdb/es"
)

// CreateIndex 创建索引
func CreateIndex(index, mapping string) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	ctx := context.Background()

	exists, err := db.IndexExists(index).Do(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	info, err := db.CreateIndex(index).BodyString(mapping).Do(ctx)
	if err != nil {
		return err
	}

	if !info.Acknowledged {
		return errors.New(fmt.Sprintf("failed to create index %s", index))
	}

	return nil
}

// DeleteIndex 删除索引
func DeleteIndex(index string) error {
	db, err := es.GetEsDB()
	if err != nil {
		return err
	}

	info, err := db.DeleteIndex(index).Do(context.Background())
	if err != nil {
		return err
	}

	if !info.Acknowledged {
		return errors.New(fmt.Sprintf("failed to delete index %s", index))
	}

	return nil
}
