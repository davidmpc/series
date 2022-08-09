package main

import (
	"flag"
	"net/http"
	"regexp"
	"time"
)

var queryTimeout = flag.Duration("queryTimeout", time.Minute, "maximum amount of a query is allowed to process")

type routeHandler func(parts []string, w http.ResponseWriter, req *http.Request)

type routingEntry struct {
	Method   string
	Path     *regexp.Regexp
	Handler  routeHandler
	Deadline time.Duration
}

const dbMatch = "[-%+()$_a-zA-Z0-9]+"

var defaultDeadline = time.Millisecond * 50

var routingTable []routingEntry

func init() {
	routingTable = []routingEntry{
		{"GET", regexp.MustCompile("^/$"), serverInfo, defaultDeadline},
		{"GET", regexp.MustCompile("^/_static/(.*)"), staticHandler, defaultDeadline},
		{"GET", regexp.MustCompile("^/_debug/open$"), debugListOpenDBs, defaultDeadline},
		{"GET", regexp.MustCompile("^/_debug/vars"), debugVars, defaultDeadline},
		{"GET", regexp.MustCompile("^/_all_dbs$"), listDataBases, defaultDeadline},
		{"GET", regexp.MustCompile("^/_(.*)"), reservedHandler, defaultDeadline},
		{"GET", regexp.MustCompile("^/(" + dbMatch + ")/?$"), dbInfo, defaultDeadline},
		{"HEAD", regexp.MustCompile("^/(" + dbMatch + ")/?$"), checkDB, defaultDeadline},
		{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_changes$"), dbChanges, defaultDeadline},
		{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_query$"), query, defaultDeadline},
		{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/_bulks"), deleteBulk, *queryTimeout},
		{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_all"), allDocs, *queryTimeout},
		{"GET", regexp.MustCompile("^/(" + dbMatch + ")/_dump"), dumpDocs, *queryTimeout},
		{"POST", regexp.MustCompile("^/(" + dbMatch + ")/_compact"), compact, time.Second * 30},
		{"PUT", regexp.MustCompile("^/(" + dbMatch + ")/?$"), createDB, defaultDeadline},
		{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/?$"), deleteDB, defaultDeadline},
		{"POST", regexp.MustCompile("^/(" + dbMatch + ")/?$"), newDoc, defaultDeadline},
		{"PUT", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"), putDoc, defaultDeadline},
		{"GET", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"), getDoc, defaultDeadline},
		{"DELETE", regexp.MustCompile("^/(" + dbMatch + ")/([^/]+)$"), rmDoc, defaultDeadline},
		{"OPTIONS", regexp.MustCompile(".*"), optionsHandler, defaultDeadline},
	}
}

func mustEncode(status int, w http.ResponseWriter, ob interface{}) {

}
