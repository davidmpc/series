package main

import (
	"errors"
	"github.com/mschoch/gouchstore"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type dbOperation uint8

const (
	opStoreItem = dbOperation(iota)
	opDeleteItem
	opCompact
)

const dbExt = ".couch"

type dbqitem struct {
	dbname string
	k      string
	data   []byte
	op     dbOperation
	cherr  chan error
}

type dbWriter struct {
	dbname string
	ch     chan dbqitem
	quit   chan bool
	db     *gouchstore.Gouchstore
}

var errClosed = errors.New("closed")

func (w *dbWriter) Close() error {
	select {
	case <-w.quit:
		return errClosed
	default:
	}
	close(w.quit)
	return nil
}

var dbLock = sync.Mutex{}
var dbConns = map[string]*dbWriter{}

func dbPath(name string) string {
	return filepath.Join(*dbRoot, name) + dbExt
}

func dbBase(n string) string {
	left := 0
	right := len(n)
	if strings.HasPrefix(n, *dbRoot) {
		left = len(*dbRoot)
		if n[left] == '/' {
			left++
		}
	}
	if strings.HasSuffix(n, dbExt) {
		right = len(n) - len(dbExt)
	}
	return n[left:right]
}

func dbOpen(name string) (*gouchstore.Gouchstore, error) {
	path := dbPath(name)
	db, err := gouchstore.Open(dbPath(name), 0)
	if err == nil {
		recordDBConn(path, db)
	}
	return db, err
}

func dbcreate(path string) error {
	db, err := gouchstore.Open(path, gouchstore.OPEN_CREATE)
	if err != nil {
		return err
	}
	recordDBConn(path, db)
	closeDBConn(db)
	return nil
}

func dbRemoveConn(name string) {
	dbLock.Lock()
	defer dbLock.Unlock()

	writer := dbConns[name]
	if writer != nil && writer.quit != nil {
		writer.Close()
	}
	delete(dbConns, name)
}

func dbCloseAll() {
	dbLock.Lock()
	defer dbLock.Unlock()

	for n, c := range dbConns {
		log.Printf("shutting down open conn %s", n)
		c.Close()
	}
}

func dbdelete(name string) error {
	dbRemoveConn(name)
	return os.Remove(dbPath(name))
}

func dblist(root string) []string {
	rv := []string{}
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil {
			if !info.IsDir() && strings.HasSuffix(p, dbExt) {
				rv = append(rv, dbBase(p))
			}
		} else {
			log.Printf("error on %#v: %v", p, err)
		}
		return nil
	})
	return rv
}

func dbCompact(dq *dbWriter, bulk gouchstore.BulkWriter, queued int, qi dbqitem) (gouchstore.BulkWriter, error) {
	start := time.Now()
	if queued > 0 {
		bulk.Commit()
		log.Printf("flushed %d items in %v for per-compact", queued, time.Since(start))
		bulk.Close()
	}
	dbn := dbPath(dq.dbname)
	queued = 0
	start = time.Now()
	err := dq.db.Compact(dbn + ".compact")
	if err != nil {
		log.Printf("error compacting : %v", err)
		return dq.db.Bulk(), err
	}
	log.Printf("finished compaction of %v in %v", dq.dbname, time.Since(start))
	err = os.Rename(dbn+".compact", dbn)
	if err != nil {
		log.Printf("error putting compacted data back")
		return dq.db.Bulk(), err
	}
	log.Printf("reopening post-compact")
	closeDBConn(dq.db)

	dq.db, err = dbOpen(dq.dbname)
	if err != nil {
		log.Fatalf("error reopening DB after compaction : %v", err)
	}
	return dq.db.Bulk(), nil
}

var dbWg = sync.WaitGroup{}

func dbWriteLoop(dq *dbWriter) {
	defer dbWg.Done()

	queued := 0
	bulk := dq.db.Bulk()

	t := time.NewTimer(*flushTime)
	defer t.Stop()
	liveTracker := time.NewTicker(*liveTime)
	defer liveTracker.Stop()
	liveOps := 0

	dbst := dbStats.getOrCreate(dq.dbname)
	defer atomic.StoreUint32(&dbst.qlen, 0)
	defer atomic.AddUint32(&dbst.closes, 1)

	for {
		atomic.StoreUint32(&dbst.qlen, uint32(queued))
		select {
		case <-dq.quit:
			sdt := time.Now()
			bulk.Commit()
			bulk.Close()
			closeDBConn(dq.db)
		}
	}
}