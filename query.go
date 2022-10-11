package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-jsonpointer"
	"log"
	"math"
	"reflect"
	"sync/atomic"
	"time"

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

func resolveFetch(doc []byte, keys []string) map[string]interface{} {
	ret := map[string]interface{}{}
	found, err := jsonpointer.FindMany(doc, keys)
	if err != nil {
		return ret
	}
	for k, v := range found {
		var val interface{}
		err = json.Unmarshal(v, &val)
		if err != nil {
			ret[k] = val
		}
	}
	return ret
}

func processDoc(di *gouchstore.DocumentInfo, chs []chan ptrval, doc []byte,
	ptrs []string, filters []string, filtervals []string, included bool) {
	seen := map[string]bool{}
	var keys []string
	for _, p := range ptrs {
		if !seen[p] {
			seen[p] = true
			keys = append(keys, p)
		}
	}
	for _, f := range filters {
		if !seen[f] {
			seen[f] = true
			keys = append(keys, f)
		}
	}
	fetched := resolveFetch(doc, keys)
	for i, p := range filters {
		val := fetched[p]
		checkval := filtervals[i]
		switch val.(type) {
		case string:
			if val != checkval {
				return
			}
		case int, uint, uint64, int64, float64:
			v := fmt.Sprintf("%v", val)
			if v != checkval {
				return
			}
		default:
			return
		}
	}
	pv := ptrval{
		di:       di,
		val:      nil,
		included: included,
	}
	for i, p := range ptrs {
		val := fetched[p]
		if p == "_id" {
			val = di.ID
		}
		switch x := val.(type) {
		case int, uint, int64, uint64, float64:
			v := fmt.Sprintf("%v", x)
			pv.val = v
			chs[i] <- pv
		default:
			pv.val = x
			chs[i] <- pv
		}
	}
}

func processDocs(pi *processIn) {
	result := processOut{pi.key, nil, nil, pi.cacheKey, 0}

	if len(pi.ptrs) == 0 {
		log.Panicf("No pointers specified in query, %#v", pi)
	}
	db, err := dbopen(pi.dbname)
	if err != nil {
		result.err = err
		pi.out <- &result
		return
	}
	defer dbclose(db)

	chans := make([]chan ptrval, 0, len(pi.ptrs))
	resultchans := make([]chan interface{}, 0, len(pi.ptrs))
	for i, r := range pi.reds {
		chans[i] = make(chan ptrval)
		resultchans[i] = make(chan interface{})

		go func(fi int, fr string) {
			resultchans[fi] <- reducers[fr](chans[fi])
		}(i, r)
	}

	go func() {
		closeAll := func(chans interface{}) {
			v := reflect.ValueOf(chans)
			for i, imax := 0, v.Len(); i < imax; i++ {
				v.Index(i).Close()
			}
		}
		defer closeAll(chans)
		doDoc := func(di *gouchstore.DocumentInfo, included bool) {
			doc, err := db.DocumentByDocumentInfo(di)
			if err != nil {
				for i := range pi.ptrs {
					chans[i] <- ptrval{di, nil, included}
				}
			} else {
				processDoc(di, chans, doc.Body, pi.ptrs, pi.filters, pi.filtervals, included)
			}
		}

		for _, di := range pi.infos {
			doDoc(di, true)
		}
		if pi.nextInfo != nil {
			doDoc(pi.nextInfo, false)
		}
	}()

	results := make([]interface{}, len(pi.ptrs))
	for i := range pi.ptrs {
		results[i] = <-resultchans[i]
		if f, fok := results[i].(float64); fok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			results[i] = 0
		}
	}
	result.value = results

	if result.cacheOpaque == 0 && result.cacheKey != "" {
		select {
		case cacheInputSet <- &result:
		default:
		}
	}
	pi.out <- &result
}

func docProcessor(ch <-chan *processIn) {
	for pi := range ch {
		if time.Now().Before(pi.before) {
			processDocs(pi)
		} else {
			pi.out <- &processOut{pi.key, nil, errTimeout, "", 0}
		}
	}
}

func fetchDocs(dbname string, key int64, infos []*gouchstore.DocumentInfo,
	nextInfo *gouchstore.DocumentInfo, ptrs []string, reds []string, filters []string,
	filtervals []string, before time.Time, out chan<- *processOut) {

	i := processIn{infos, nextInfo, key, dbname, "", ptrs, reds, filters,
		filtervals, before, out}
	cacheInput <- &i
}

func runQuery(q *queryIn) {
	if len(q.ptrs) == 0 {
		q.cherr <- fmt.Errorf("at least one query is required")
		return
	}
	if q.group == 0 {
		q.cherr <- fmt.Errorf("group level can't be zero")
		return
	}

	db, err := dbopen(q.dbname)
	if err != nil {
		log.Printf("error opening db: %v - %v", q.dbname, err)
		q.cherr <- err
		return
	}
	defer dbclose(db)

	chunk := int64(time.Duration(q.group) * time.Millisecond)

	info := []*gouchstore.DocumentInfo{}
	g := int64(0)
	nextg := ""

	err = db.AllDocuments(q.from, q.to, func(db *gouchstore.Gouchstore, di *gouchstore.DocumentInfo,
		userContext interface{}) error {
		kstr := di.ID
		var err error

		atomic.AddInt32(&q.totalKeys, 1)
	}, nil)
}

var reducers = map[string]reducer{

}