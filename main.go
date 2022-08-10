package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"time"
)

var queryTimeout = flag.Duration("queryTimeout", time.Minute, "maximum amount of a query is allowed to process")
var staticPath = flag.String("static", "static", "path to static data")

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
	b, err := json.Marshal(ob)
	if err != nil {
		log.Fatalf("error encoding %v", ob)
	}
	w.WriteHeader(status)
	w.Header().Set("Content-length", fmt.Sprintf("%d", len(b)))
	w.Write(b)
}

func emitError(status int, w http.ResponseWriter, e, reason string) {
	m := map[string]string{"error": e, "reason": reason}
	mustEncode(status, w, m)
}

func staticHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	w.Header().Del("Content-type")
	http.StripPrefix("/_static", http.FileServer(http.Dir(*staticPath))).ServeHTTP(w, req)
}

func reservedHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	emitError(400, w, "illegal_database_name", "must begin with a letter")
}

func defaultHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	emitError(400, w, "no_handler", fmt.Sprintf("Can't handle %v to %v\n", req.Method, req.URL.Path))
}

func optionsHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	methods := []string{}
	for _, r :=
}
