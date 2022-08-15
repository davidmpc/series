package main

import (
	"errors"
	"github.com/mschoch/gouchstore"
	"path/filepath"
	"strings"
	"sync"
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

func dbPath(s string) string {
	return filepath.Join(*dbRoot, s) + dbExt
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
