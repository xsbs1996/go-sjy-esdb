package esdb

import (
	"encoding/json"
	"errors"
	"reflect"
)

// 将返回的数据转换为结构体
func data2Resp(data []byte, resp interface{}) error {
	reflectValue := reflect.ValueOf(resp)
	if reflectValue.Kind() != reflect.Ptr {
		return errors.New("resp not is prt")
	}
	if reflectValue.IsNil() {
		return errors.New("resp is nil prt")
	}

	if err := json.Unmarshal(data, &resp); err != nil {
		return err
	}

	return nil
}
