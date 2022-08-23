package main

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dustin/go-jsonpointer"
	"github.com/mschoch/gouchstore"
)

var errTimeout = errors.New("query timeout")

type ptrval struct {
	di       *gouchstore.DocumentInfo
	val      interface{}
	included bool
}

type reducer func(input chan ptrval) interface{}

type processOut struct {
	key         int64
	value       []interface{}
	err         error
	cacheKey    string
	cacheOpaque uint32
}

func (p processOut) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{"v": p.value})
}

type processIn struct {
	infos      []*gouchstore.DocumentInfo
	nextInfo   *gouchstore.DocumentInfo
	key        int64
	dbname     string
	cacheKey   string
	ptrs       []string
	reds       []string
	filters    []string
	filtervals []string
	before     time.Time
	out        chan<- *processOut
}

type queryIn struct {
	dbname     string
	from       string
	to         string
	group      int
	start      time.Time
	before     time.Time
	ptrs       []string
	reds       []string
	filters    []string
	filtervals []string
	started    int32
	totalKeys  int32
	out        chan *processOut
	cherr      chan error
}

func resolveFetch(b []byte, keys []string) map[string]interface{} {
	rv := map[string]interface{}{}
	found, err := jsonpointer.FindMany(b, keys)
	if err != nil {
		return rv
	}
	for k, v := range found {
		var val interface{}
		err = json.Unmarshal(v, &val)
		if err == nil {
			rv[k] = v
		}
	}
	return rv
}

func processDoc(di *gouchstore.DocumentInfo, chs []chan ptrval, doc []byte, ptrs []string, filters []string,
	filtervals []string, included bool) {

}
