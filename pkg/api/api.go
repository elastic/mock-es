package api

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mileusna/useragent"
	"github.com/rcrowley/go-metrics"
)

// BulkResponse is an Elastic Search Bulk Response, assuming
// filter_path is "errors,items.*.error,items.*.status"
type BulkResponse struct {
	Errors bool             `json:"errors"`
	Items  []map[string]any `json:"items,omitempty"`
}

// APIHandler struct.  Use NewAPIHandler to make sure it is filled in correctly for use.
type APIHandler struct {
	ActionOdds    [100]int
	MethodOdds    [100]int
	UUID          uuid.UUID
	ClusterUUID   string
	Expire        time.Time
	Delay         time.Duration
	bulkTotal     metrics.Counter
	bulkDuplicate metrics.Counter
	bulkTooMany   metrics.Counter
	bulkNonIndex  metrics.Counter
	bulkOK        metrics.Counter
	bulkTooLarge  metrics.Counter
	bulkIndex     metrics.Counter
	bulkUpdate    metrics.Counter
	bulkDelete    metrics.Counter
	licenseTotal  metrics.Counter
	rootTotal     metrics.Counter
}

// NewAPIHandler return handler with Action and Method Odds array filled in
func NewAPIHandler(uuid uuid.UUID, clusterUUID string, metricsRegistry metrics.Registry, expire time.Time, delay time.Duration, percentDuplicate, percentTooMany, percentNonIndex, percentTooLarge uint) *APIHandler {
	h := &APIHandler{UUID: uuid, Expire: expire, ClusterUUID: clusterUUID, Delay: delay}
	if int((percentDuplicate + percentTooMany + percentNonIndex)) > len(h.ActionOdds) {
		panic(fmt.Errorf("Total of percents can't be greater than %d", len(h.ActionOdds)))
	}
	if int(percentTooLarge) > len(h.MethodOdds) {
		panic(fmt.Errorf("percent TooLarge cannot be greater than %d", len(h.MethodOdds)))
	}

	// Fill in ActionOdds
	n := 0
	for i := uint(0); i < percentDuplicate; i++ {
		h.ActionOdds[n] = http.StatusConflict
		n++
	}
	for i := uint(0); i < percentTooMany; i++ {
		h.ActionOdds[n] = http.StatusTooManyRequests
		n++
	}
	for i := uint(0); i < percentNonIndex; i++ {
		h.ActionOdds[n] = http.StatusNotAcceptable
		n++
	}
	for ; n < len(h.ActionOdds); n++ {
		h.ActionOdds[n] = http.StatusOK
	}

	// Fill in MethodOdds
	n = 0
	for i := uint(0); i < percentTooLarge; i++ {
		h.MethodOdds[n] = http.StatusRequestEntityTooLarge
		n++
	}
	for ; n < len(h.MethodOdds); n++ {
		h.MethodOdds[n] = http.StatusOK
	}
	bulkRegistry := metrics.NewPrefixedChildRegistry(metricsRegistry, "bulk.create.")

	h.bulkTotal = metrics.NewCounter()
	bulkRegistry.Register("total", h.bulkTotal)
	h.bulkDuplicate = metrics.NewCounter()
	bulkRegistry.Register("duplicate", h.bulkDuplicate)
	h.bulkTooMany = metrics.NewCounter()
	bulkRegistry.Register("too_many", h.bulkTooMany)
	h.bulkNonIndex = metrics.NewCounter()
	bulkRegistry.Register("non_index", h.bulkNonIndex)
	h.bulkOK = metrics.NewCounter()
	bulkRegistry.Register("ok", h.bulkOK)
	h.bulkTooLarge = metrics.NewCounter()
	bulkRegistry.Register("too_large", h.bulkTooLarge)
	h.bulkIndex = metrics.NewCounter()
	metrics.GetOrRegister("bulk.index.total", h.bulkIndex)
	h.bulkUpdate = metrics.NewCounter()
	metrics.GetOrRegister("bulk.update.total", h.bulkUpdate)
	h.bulkDelete = metrics.NewCounter()
	metrics.GetOrRegister("bulk.delete.total", h.bulkDelete)
	h.licenseTotal = metrics.NewCounter()
	metrics.GetOrRegister("license.total", h.licenseTotal)
	h.rootTotal = metrics.NewCounter()
	metrics.GetOrRegister("root.total", h.rootTotal)
	return h
}

// ServeHTTP looks at the request and routes it to the correct handler function
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(h.Delay)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/":
		h.Root(w, r)
		return
	case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
		h.Bulk(w, r)
		return
	case r.Method == http.MethodGet && r.URL.Path == "/_license":
		h.License(w, r)
		return
	default:
		w.Write([]byte("{\"tagline\": \"You Know, for Testing\"}"))
		return
	}
}

// Bulk handles bulk posts
func (h *APIHandler) Bulk(w http.ResponseWriter, r *http.Request) {
	h.bulkTotal.Inc(1)
	methodStatus := h.MethodOdds[rand.Intn(len(h.MethodOdds))]
	if methodStatus == http.StatusRequestEntityTooLarge {
		h.bulkTooLarge.Inc(1)
		w.WriteHeader(methodStatus)
		return
	}

	var scanner *bufio.Scanner
	br := BulkResponse{}
	encoding, prs := r.Header[http.CanonicalHeaderKey("Content-Encoding")]
	switch {
	case prs && encoding[0] == "gzip":
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			log.Printf("error new gzip reader failed: %s", err)
			return
		}
		scanner = bufio.NewScanner(zr)
	default:
		scanner = bufio.NewScanner(r.Body)
	}
	// bulk requests come in as 2 lines
	// the action on first line, followed by the document on the next line.
	// we only care about the action, which is why we have skipNextLine var
	// eg:
	// { "update": {"_id": "5", "_index": "index1"} }
	// { "doc": {"my_field": "baz"} }

	var skipNextLine bool
	for scanner.Scan() {
		b := scanner.Bytes()
		if skipNextLine || len(b) == 0 {
			skipNextLine = false
			continue
		}
		var j map[string]any
		err := json.Unmarshal(b, &j)
		if err != nil {
			log.Printf("error unmarshal: %s", err)
			continue
		}
		if len(j) != 1 {
			log.Printf("error, number of keys off: %d should be 1", len(j))
			continue
		}
		for k := range j {
			switch k {
			case "index":
				h.bulkIndex.Inc(1)
				skipNextLine = true
			case "create":
				skipNextLine = true
				actionStatus := h.ActionOdds[rand.Intn(len(h.ActionOdds))]
				switch actionStatus {
				case http.StatusOK:
					h.bulkOK.Inc(1)
				case http.StatusConflict:
					br.Errors = true
					h.bulkDuplicate.Inc(1)
				case http.StatusTooManyRequests:
					br.Errors = true
					h.bulkTooMany.Inc(1)
				case http.StatusNotAcceptable:
					br.Errors = true
					h.bulkNonIndex.Inc(1)
				}
				br.Items = append(br.Items, map[string]any{"created": map[string]any{"status": actionStatus}})
			case "update":
				h.bulkUpdate.Inc(1)
				skipNextLine = true
			case "delete":
				h.bulkDelete.Inc(1)
				skipNextLine = false
			}
		}
	}
	brBytes, err := json.Marshal(br)
	if err != nil {
		log.Printf("error marshal bulk reply: %s", err)
		return
	}
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.Write(brBytes)
	return
}

// Root handles / get requests
func (h *APIHandler) Root(w http.ResponseWriter, r *http.Request) {
	h.rootTotal.Inc(1)
	version := parseUserAgent(r.Header.Get("User-Agent"))
	root := fmt.Sprintf("{\"name\" : \"mock\", \"cluster_uuid\" : \"%s\", \"version\" : { \"number\" : \"%s\", \"build_flavor\" : \"default\"}}", h.ClusterUUID, version)
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.Write([]byte(root))
	return
}

// License handles /_license get requests
func (h *APIHandler) License(w http.ResponseWriter, r *http.Request) {
	h.licenseTotal.Inc(1)
	license := fmt.Sprintf("{\"license\" : {\"status\" : \"active\", \"uid\" : \"%s\", \"type\" : \"trial\", \"expiry_date_in_millis\" : %d}}", h.UUID.String(), h.Expire.UnixMilli())
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.Write([]byte(license))
	return
}

func parseUserAgent(agentString string) string {
	ua := useragent.Parse(agentString)
	return ua.VersionNoFull()
}
