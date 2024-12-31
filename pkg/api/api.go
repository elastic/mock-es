package api

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/mileusna/useragent"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// BulkResponse is an Elastic Search Bulk Response, assuming
// filter_path is "errors,items.*.error,items.*.status"
type BulkResponse struct {
	Errors bool             `json:"errors"`
	Items  []map[string]any `json:"items,omitempty"`
}

type dataStore struct {
	// three dimensional map storing index, id and the document
	data map[string]map[string]map[string]any
}

var doc dataStore = dataStore{data: make(map[string]map[string]map[string]any)}

// APIHandler struct.  Use NewAPIHandler to make sure it is filled in correctly for use.
type APIHandler struct {
	ActionOdds  [100]int
	MethodOdds  [100]int
	UUID        fmt.Stringer
	ClusterUUID string
	Expire      time.Time
	Delay       time.Duration
	metrics     *metrics
}

// NewAPIHandler return handler with Action and Method Odds array filled in
func NewAPIHandler(uuid fmt.Stringer, clusterUUID string, meterProvider metric.MeterProvider, expire time.Time, delay time.Duration, percentDuplicate, percentTooMany, percentNonIndex, percentTooLarge uint) *APIHandler {
	h := &APIHandler{UUID: uuid, Expire: expire, ClusterUUID: clusterUUID, Delay: delay}
	if int((percentDuplicate + percentTooMany + percentNonIndex)) > len(h.ActionOdds) {
		panic(fmt.Errorf("Total of percents can't be greater than %d", len(h.ActionOdds)))
	}
	if int(percentTooLarge) > len(h.MethodOdds) {
		panic(fmt.Errorf("percent TooLarge cannot be greater than %d", len(h.MethodOdds)))
	}

	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	metrics, err := newMetrics(meterProvider)
	if err != nil {
		panic(fmt.Errorf("failed to create metrics"))
	}
	h.metrics = metrics

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

	return h
}

// ServeHTTP looks at the request and routes it to the correct handler function
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(h.Delay)

	// required for official clients to recognize this as a valid endpoint.
	w.Header().Set("X-Elastic-Product", "Elasticsearch")

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
	case r.Method == http.MethodGet && r.URL.Path == "/data":
		h.Data(w, r)
		return
	default:
		w.Write([]byte("{\"tagline\": \"You Know, for Testing\"}"))
		return
	}
}

// Bulk handles bulk posts
func (h *APIHandler) Bulk(w http.ResponseWriter, r *http.Request) {
	attrs := metric.WithAttributeSet(requestAttributes(r))
	h.metrics.bulkCreateTotalMetrics.Add(context.Background(), 1, attrs)
	methodStatus := h.MethodOdds[rand.Intn(len(h.MethodOdds))]
	if methodStatus == http.StatusRequestEntityTooLarge {
		h.metrics.bulkCreateTooLargeMetrics.Add(context.Background(), 1, attrs)
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

	var indexName string
	var id string

	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) == 0 {
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

		for k, v := range j {
			if m, ok := v.(map[string]any); ok {
				indexName = m["_index"].(string)
				id = m["_id"].(string)
			}
			switch k {
			case "index":
				h.metrics.bulkIndexTotalMetrics.Add(context.Background(), 1, attrs)
				d, err := getDocument(scanner)
				if err != nil {
					return
				}
				if doc.data[indexName] == nil {
					doc.data[indexName] = make(map[string]map[string]any)
				}
				doc.data[indexName][id] = d
				br.Items = append(br.Items, map[string]any{k: v, "result": "created", "status": 201})

			case "create":
				actionStatus := h.ActionOdds[rand.Intn(len(h.ActionOdds))]
				switch actionStatus {
				case http.StatusOK:
					h.metrics.bulkCreateOkMetrics.Add(context.Background(), 1, attrs)
				case http.StatusConflict:
					br.Errors = true
					h.metrics.bulkCreateDuplicateMetrics.Add(context.Background(), 1, attrs)
				case http.StatusTooManyRequests:
					br.Errors = true
					h.metrics.bulkCreateTooManyMetrics.Add(context.Background(), 1, attrs)
				case http.StatusNotAcceptable:
					br.Errors = true
					h.metrics.bulkCreateNonIndexMetrics.Add(context.Background(), 1, attrs)
				}
				d, err := getDocument(scanner)
				if err != nil {
					return
				}
				if doc.data[indexName] == nil {
					doc.data[indexName] = make(map[string]map[string]any)
				}
				doc.data[indexName][id] = d
				br.Items = append(br.Items, map[string]any{k: v, "result": "created", "status": actionStatus})

			case "update":
				h.metrics.bulkUpdateTotalMetrics.Add(context.Background(), 1, attrs)
				l, err := getDocument(scanner)
				if err != nil {
					return
				}
				// update the document
				for key, updatedValue := range l["doc"].(map[string]any) {
					if _, exists := doc.data[indexName][id][key]; exists {
						doc.data[indexName][id][key] = updatedValue
						br.Items = append(br.Items, map[string]any{k: v, "result": "updated", "status": 200})
					} else {
						br.Errors = true
						br.Items = append(br.Items, map[string]any{k: v, "result": "not_found", "status": 404})

					}
				}
			case "delete":
				h.metrics.bulkDeleteTotalMetrics.Add(context.Background(), 1, attrs)
				delete(doc.data[indexName], id)
			}
		}

	}
	brBytes, err := json.Marshal(br)
	if err != nil {
		log.Printf("error marshal bulk reply: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(brBytes)
}

func getDocument(s *bufio.Scanner) (map[string]any, error) {
	s.Scan()
	b := s.Bytes()
	var l map[string]any
	err := json.Unmarshal(b, &l)
	if err != nil {
		log.Printf("error unmarshal: %s", err)
		return nil, err
	}

	return l, nil
}

// Root handles / get requests
func (h *APIHandler) Root(w http.ResponseWriter, r *http.Request) {
	h.metrics.rootTotalMetrics.Add(context.Background(), 1, metric.WithAttributeSet(requestAttributes(r)))
	ua := useragent.Parse(r.Header.Get("User-Agent"))
	root := fmt.Sprintf("{\"name\" : \"mock\", \"cluster_uuid\" : \"%s\", \"version\" : { \"number\" : \"%s\", \"build_flavor\" : \"default\"}}", h.ClusterUUID, ua.VersionNoFull())
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.Write([]byte(root))
}

// License handles /_license get requests
func (h *APIHandler) License(w http.ResponseWriter, r *http.Request) {
	h.metrics.licenseTotalMetrics.Add(context.Background(), 1, metric.WithAttributeSet(requestAttributes(r)))
	license := fmt.Sprintf("{\"license\" : {\"status\" : \"active\", \"uid\" : \"%s\", \"type\" : \"trial\", \"expiry_date_in_millis\" : %d}}", h.UUID.String(), h.Expire.UnixMilli())
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(license))
}

// Data handles /data get requests
func (h *APIHandler) Data(w http.ResponseWriter, r *http.Request) {
	brBytes, err := json.Marshal(doc.data)
	if err != nil {
		log.Printf("error marshal bulk reply: %s", err)
		return
	}
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.Write(brBytes)
}

func requestAttributes(r *http.Request) attribute.Set {
	return attribute.NewSet(
		attribute.String("user_agent", r.UserAgent()),
		attribute.String("path", r.URL.Path),
	)
}
