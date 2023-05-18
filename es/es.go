package es

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/olivere/elastic/v7"
)

type Conf struct {
	Protocol string
	IpPorts  string
	Username string
	Password string
	Sniff    bool
}

var (
	once sync.Once
	esDB *elastic.Client
)

// InitEsDB 初始化es
func InitEsDB(conf *Conf) {
	once.Do(func() {
		IpPort := strings.Split(conf.IpPorts, ",")

		var urls []string
		for _, v := range IpPort {
			elastic.SetURL(fmt.Sprintf("%s://%s", conf.Protocol, v))
		}

		client, err := elastic.NewClient(
			elastic.SetURL(urls...),
			elastic.SetBasicAuth(conf.Username, conf.Password),
			elastic.SetSniff(conf.Sniff),
		)

		if err != nil {
			panic(fmt.Sprintf("elastic error:%s", err.Error()))
		}
		esDB = client
	})
}

// GetEsDB 获取es链接
func GetEsDB() (*elastic.Client, error) {
	if esDB == nil {
		return nil, errors.New("esDB is nil")
	}

	return esDB, nil
}
