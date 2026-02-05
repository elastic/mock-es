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
	"sync"
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

// Action is the action for /_bulk requests.
type Action struct {
	Action string
	Meta   json.RawMessage
}

// RequestRecord is a record of a request
type RequestRecord struct {
	Method string `json:"method"`
	URI    string `json:"uri"`
	Body   string `json:"body"`
}

// APIHandler struct.  Use NewAPIHandler to make sure it is filled in correctly for use.
type APIHandler struct {
	ActionOdds   [100]int
	MethodOdds   [100]int
	UUID         fmt.Stringer
	ClusterUUID  string
	Expire       time.Time
	Delay        time.Duration
	metrics      *metrics
	history      []*RequestRecord
	historyIndex int
	historyMu    sync.Mutex
	configMu     sync.RWMutex

	deterministicHandler func(action Action, event []byte) int
}

// NewAPIHandler return handler with Action and Method Odds array filled in
func NewAPIHandler(
	uuid fmt.Stringer,
	clusterUUID string,
	meterProvider metric.MeterProvider,
	expire time.Time,
	delay time.Duration,
	percentDuplicate,
	percentTooMany,
	percentNonIndex,
	percentTooLarge,
	historyCap uint,
) *APIHandler {

	// Always set a cluster UUID, so things like Beats monitoring always work
	if clusterUUID == "" {
		clusterUUID = "580445a9-f89f-429f-a84f-14956e5ad968"
	}

	h, err := newAPIHandler(uuid, clusterUUID, meterProvider, expire, delay, historyCap)
	if err != nil {
		panic(fmt.Errorf("failed to create APIHandler: %w", err))
	}

	err = h.UpdateOdds(percentDuplicate, percentTooMany, percentNonIndex, percentTooLarge)
	if err != nil {
		panic(fmt.Errorf("failed to UpdateOdds: %w", err))
	}

	return h
}

// NewDeterministicAPIHandler returns a handler which uses handler to process
// each action in the bulk request.
func NewDeterministicAPIHandler(
	uuid fmt.Stringer,
	clusterUUID string,
	meterProvider metric.MeterProvider,
	expire time.Time,
	delay time.Duration,
	historyCap uint,
	handler func(action Action, event []byte) int,
) *APIHandler {

	h, err := newAPIHandler(uuid, clusterUUID, meterProvider, expire, delay, historyCap)
	if err != nil {
		panic(fmt.Errorf("failed to create APIHandler: %w", err))
	}

	h.deterministicHandler = handler

	return h
}

func newAPIHandler(
	uuid fmt.Stringer,
	clusterUUID string,
	meterProvider metric.MeterProvider,
	expire time.Time,
	delay time.Duration,
	historyCap uint) (*APIHandler, error) {

	h := &APIHandler{
		UUID: uuid, Expire: expire, ClusterUUID: clusterUUID, Delay: delay}
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	metrics, err := newMetrics(meterProvider)
	if err != nil {
		panic(fmt.Errorf("failed to create metrics"))
	}
	h.metrics = metrics

	h.history = make([]*RequestRecord, historyCap)
	return h, err
}

func (h *APIHandler) UpdateOdds(
	percentDuplicate,
	percentTooMany,
	percentNonIndex,
	percentTooLarge uint,
) error {
	h.configMu.Lock()
	defer h.configMu.Unlock()

	if int(percentDuplicate+percentTooMany+percentNonIndex) > len(h.ActionOdds) {
		return fmt.Errorf("total of percents can't be greater than %d", len(h.ActionOdds))
	}
	if int(percentTooLarge) > len(h.MethodOdds) {
		return fmt.Errorf("percent TooLarge cannot be greater than %d", len(h.MethodOdds))
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

	return nil
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
	case r.Method == http.MethodGet && r.URL.Path == "/_history":
		h.History(w, r)
		return
	default:
		w.Write([]byte("{\"tagline\": \"You Know, for Testing\"}"))
		return
	}
}

// Bulk handles bulk posts
func (h *APIHandler) Bulk(w http.ResponseWriter, r *http.Request) {
	h.configMu.RLock()
	defer h.configMu.RUnlock()

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

	var body []byte
	for scanner.Scan() {
		b := scanner.Bytes()
		body = append(body, b...)
		if len(b) == 0 {
			continue
		}

		action, err := h.parseAction(b)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(
				fmt.Sprintf(`{"error": "failed to parse action: %v"}`, err)))
			log.Printf("failed to parse action: %v", err)
			return
		}

		// read document for the action. Delete action does not contain a document
		b = nil
		if action.Action != "delete" && scanner.Scan() {
			b = scanner.Bytes()
			body = append(body, b...)
		}

		var actionStatus int
		var item map[string]any
		if h.deterministicHandler != nil {
			actionStatus = h.deterministicHandler(action, b)
			item = map[string]any{action.Action: map[string]any{"status": actionStatus}}
		} else if action.Action == "create" {
			// this is the probabilistic handler, it does nothing for all the
			// other actions, only create is handled according to the odds.
			actionStatus = h.ActionOdds[rand.Intn(len(h.ActionOdds))]
			item = map[string]any{action.Action: map[string]any{"status": actionStatus}}
		}

		isErr := h.updateMetrics(action.Action, actionStatus, attrs)
		br.Errors = br.Errors || isErr
		if len(item) > 0 {
			br.Items = append(br.Items, item)
		}
	}

	h.recordRequest(r, body)
	brBytes, err := json.Marshal(br)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(
			fmt.Sprintf(`{"error": "error marshal bulk reply: %v"}`, err)))
		log.Printf("error marshal bulk reply: %s", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(brBytes)

	return
}

func (h *APIHandler) parseAction(b []byte) (Action, error) {
	var j map[string]json.RawMessage
	err := json.Unmarshal(b, &j)
	if err != nil {
		return Action{}, fmt.Errorf("error unmarshal: %s", err)

	}
	if len(j) != 1 {
		return Action{}, fmt.Errorf(
			"error, unexpected number of keys: got %d, it should be 1", len(j))
	}

	for action, data := range j {
		return Action{
			Action: action,
			Meta:   data,
		}, nil
	}

	return Action{}, fmt.Errorf("unexpected error: no action found")
}

func (h *APIHandler) updateMetrics(action string, actionStatus int, attrs metric.MeasurementOption) bool {
	var isErr bool

	switch action {
	case "index":
		h.metrics.bulkIndexTotalMetrics.Add(context.Background(), 1, attrs)
	case "create":
		switch actionStatus {
		case http.StatusOK:
			h.metrics.bulkCreateOkMetrics.Add(context.Background(), 1, attrs)
		case http.StatusConflict:
			isErr = true
			h.metrics.bulkCreateDuplicateMetrics.Add(context.Background(), 1, attrs)
		case http.StatusTooManyRequests:
			isErr = true
			h.metrics.bulkCreateTooManyMetrics.Add(context.Background(), 1, attrs)
		case http.StatusNotAcceptable:
			isErr = true
			h.metrics.bulkCreateNonIndexMetrics.Add(context.Background(), 1, attrs)
		}
	case "update":
		h.metrics.bulkUpdateTotalMetrics.Add(context.Background(), 1, attrs)
	case "delete":
		h.metrics.bulkDeleteTotalMetrics.Add(context.Background(), 1, attrs)
	}

	return isErr
}

// Root handles / get requests
func (h *APIHandler) Root(w http.ResponseWriter, r *http.Request) {
	h.recordRequest(r, nil)
	h.metrics.rootTotalMetrics.Add(context.Background(), 1, metric.WithAttributeSet(requestAttributes(r)))
	ua := useragent.Parse(r.Header.Get("User-Agent"))
	root := fmt.Sprintf("{\"name\" : \"mock\", \"cluster_uuid\" : \"%s\", \"version\" : { \"number\" : \"%s\", \"build_flavor\" : \"default\"}}", h.ClusterUUID, ua.VersionNoFull())
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.Write([]byte(root))
	return
}

// License handles /_license get requests
func (h *APIHandler) License(w http.ResponseWriter, r *http.Request) {
	h.recordRequest(r, nil)
	h.metrics.licenseTotalMetrics.Add(context.Background(), 1, metric.WithAttributeSet(requestAttributes(r)))
	license := fmt.Sprintf("{\"license\" : {\"status\" : \"active\", \"uid\" : \"%s\", \"type\" : \"trial\", \"expiry_date_in_millis\" : %d}}", h.UUID.String(), h.Expire.UnixMilli())
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.Write([]byte(license))
	return
}

// History handles /_history get requests
func (h *APIHandler) History(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
	w.WriteHeader(http.StatusOK)

	h.historyMu.Lock()
	defer h.historyMu.Unlock()

	nonNilHist := make([]RequestRecord, 0)
	for _, v := range h.history {
		if v != nil {
			nonNilHist = append(nonNilHist, *v)
		}
	}

	json.NewEncoder(w).Encode(nonNilHist)
	return
}

// RequestHistory returns a list of all requests made to the handler
func (h *APIHandler) RequestHistory() []*RequestRecord {
	h.historyMu.Lock()
	defer h.historyMu.Unlock()
	return h.history
}

func (h *APIHandler) recordRequest(r *http.Request, body []byte) {
	if cap(h.history) == 0 {
		return
	}
	h.historyMu.Lock()
	defer h.historyMu.Unlock()
	h.history[h.historyIndex] = &RequestRecord{Method: r.Method, URI: r.URL.RequestURI(), Body: string(body)}
	h.historyIndex = (h.historyIndex + 1) % cap(h.history)
}

func requestAttributes(r *http.Request) attribute.Set {
	return attribute.NewSet(
		attribute.String("user_agent", r.UserAgent()),
		attribute.String("path", r.URL.Path),
	)
}
