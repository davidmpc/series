package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/syslog"
	"net"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/dustin/yellow"
)

var docBacklog = flag.Int("docBacklog", 0, "MR group request backlog size")
var queryBacklog = flag.Int("queryBacklog", 0, "query scan/group backlog size")
var cacheAddr = flag.String("memcache", "", "memcached server to connect to")
var cacheWorkers = flag.Int("cacheWorkers", 4, "num of cache workers")
var cacheBacklog = flag.Int("cacheBacklog", 1000, "cache backlog size")
var dbRoot = flag.String("root", "db", "root directory for database files")
var pprofStart = flag.Duration("pprofStart", time.Second*1, "time to start profile")
var pprofFile = flag.String("pprofFile", "pprofFile", "file to write profiling info into")
var pprofDuration = flag.Duration("pprofDuration", time.Minute*5, "how long to run cpu profiler before "+
	"shutting it down")
var logAccess = flag.Bool("logAccess", false, "log access")
var queryTimeout = flag.Duration("queryTimeout", time.Minute, "maximum amount of a query is allowed to process")
var staticPath = flag.String("static", "static", "path to static data")
var addr = flag.String("addr", ":3133", "address to bind to")
var mcaddr = flag.String("memcbind", "", "")
var useSyslog = flag.Bool("useSyslog", true, "log to syslog")

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
	for _, r := range routingTable {
		if len(r.Path.FindAllStringSubmatch(req.URL.Path, 1)) > 0 {
			methods = append(methods, r.Method)
		}
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ", "))
	w.WriteHeader(204)
}

func findHandler(method, path string) (routingEntry, []string) {
	for _, r := range routingTable {
		if r.Method == method {
			matches := r.Path.FindAllStringSubmatch(path, 1)
			if len(matches) > 0 {
				return r, matches[0][1:]
			}
		}
	}
	return routingEntry{"DEFAULT", nil, defaultHandler, defaultDeadline}, []string{}
}

func handler(w http.ResponseWriter, req *http.Request) {
	if *logAccess {
		log.Printf("%s %s %v", req.RemoteAddr, req.Method, req.URL)
	}
	route, hparts := findHandler(req.Method, req.URL.Path)
	defer yellow.DeadlineLog(route.Deadline, "%v:%v deadlined at %v", req.Method, req.URL.Path, route.Deadline).Done()

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-type", "application/json")
	route.Handler(hparts, w, req)
}

func startProfile() {
	time.Sleep(*pprofStart)
	log.Printf("start profiler")
	f, err := os.OpenFile(*pprofFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("can't open profile file")
		return
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Fatalf("can't start profiler")
	}
	time.AfterFunc(*pprofDuration, func() {
		log.Printf("shuting down profiler")
		pprof.StopCPUProfile()
		f.Close()
	})
}

var globalShutdownChan = make(chan bool)

func listener(addr string) net.Listener {
	if addr == "" {
		addr = ":http"
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("error setting up listener:%v", err)
	}
	return l
}

func shutdownHandler(ls []net.Listener, ch <-chan os.Signal) {
	s := <-ch
	log.Printf("shutting down on sig %v", s)
	for _, l := range ls {
		l.Close()
	}
	dbCloseAll()
	time.AfterFunc(time.Second*10, func() {
		log.Fatalf("timed out waiting for connections to close")
	})
}

func main() {
	halfProcs := runtime.GOMAXPROCS(0) / 2
	if halfProcs < 1 {
		halfProcs = 1
	}
	queryWorkers := flag.Int("queryWorkers", halfProcs, "number of query tree walkers")
	docWorkers := flag.Int("docWorkers", halfProcs, "number of document mapreduce workers")
	flag.Parse()

	if *useSyslog {
		sl, err := syslog.New(syslog.LOG_INFO, "series")
		if err != nil {
			log.Fatalf("can't initialize syslog: %v", err)
		}
		log.SetOutput(sl)
		log.SetFlags(0)
	}

	if err := os.MkdirAll(*dbRoot, 0777); err != nil {
		log.Fatalf("could not create %v: %v", *dbRoot, err)
	}

	found := false
	for i := range routingTable {
		matches := routingTable[i].Path.FindAllStringSubmatch("/x/_query", 1)
		if len(matches) > 0 {
			routingTable[i].Deadline = *queryTimeout
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("error, count't find query handler")
	}

	processorInput = make(chan *processIn, *docBacklog)
	for i := 0; i < *docWorkers; i++ {
		go docProcessor(processorInput)
	}

	if *cacheAddr == "" {
		cacheInput = processorInput
	} else {
		cacheInput = make(chan *processIn, *cacheBacklog)
		cacheInputSet = make(chan *processOut, *cacheBacklog)
		for i := 0; i < *cacheWorkers; i++ {
			go cacheProcessor(cacheInput, cacheInputSet)
		}
	}
}
