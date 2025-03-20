package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/mock-es/pkg/api"
	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var (
	addr             string
	expire           time.Time
	percentDuplicate uint
	percentTooMany   uint
	percentNonIndex  uint
	percentTooLarge  uint
	historyCap       uint
	uid              uuid.UUID
	clusterUUID      string
	metricsInterval  time.Duration
	certFile         string
	keyFile          string
	delay            time.Duration
	verbose          bool
)

func init() {
	flag.StringVar(&addr, "addr", ":9200", "address to listen on ip:port")
	flag.UintVar(&percentDuplicate, "dup", 0, "percent chance StatusConflict is returned for create action")
	flag.UintVar(&percentTooMany, "toomany", 0, "percent chance StatusTooManyRequests is returned for create action")
	flag.UintVar(&percentNonIndex, "nonindex", 0, "percent chance StatusNotAcceptable is returned for create action")
	flag.UintVar(&percentTooLarge, "toolarge", 0, "percent chance StatusEntityTooLarge is returned for POST method on _bulk endpoint")
	flag.UintVar(&historyCap, "history", 0, "number of request bodies to keep, available on _history endpoint")
	flag.StringVar(&clusterUUID, "clusteruuid", "", "Cluster UUID of Elasticsearch we are mocking")
	flag.DurationVar(&metricsInterval, "metrics", 0, "Go 'time.Duration' to wait between printing metrics to stdout, 0 is no metrics")
	flag.StringVar(&certFile, "certfile", "", "path to PEM certificate file, empty sting is no TLS")
	flag.StringVar(&keyFile, "keyfile", "", "path to PEM private key file, empty sting is no TLS")
	flag.DurationVar(&delay, "delay", 0, "Go 'time.Duration' to wait before processing API request, 0 is no delay")
	flag.BoolVar(&verbose, "verbose", false, "Enable verbosity, show non error messages")

	uid = uuid.Must(uuid.NewV4())
	expire = time.Now().Add(24 * time.Hour)
	flag.Parse()
	if (percentDuplicate + percentTooMany + percentNonIndex) > 100 {
		log.Fatalf("Total of create action percentages must not be more than 100.\nd: %d, t:%d, n:%d", percentDuplicate, percentTooMany, percentNonIndex)
	}
	if percentTooLarge > 100 {
		log.Fatalf("percentage StatusEntityTooLarge must be less than 100")
	}
}

func main() {
	mux := http.NewServeMux()
	var provider metric.MeterProvider

	if metricsInterval > 0 {
		rdr := sdkmetric.NewManualReader()
		provider = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(rdr),
		)

		go func() {
			for range time.Tick(metricsInterval) {
				rm := metricdata.ResourceMetrics{}
				err := rdr.Collect(context.Background(), &rm)

				if err != nil {
					log.Fatalf("failed to collect metrics: %v", err)
				}

				for _, sm := range rm.ScopeMetrics {
					type Value struct {
						Count int64 `json:"count,omitempty"`
					}
					out := make(map[string]Value, len(sm.Metrics))
					for _, m := range sm.Metrics {
						for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
							out[m.Name] = Value{
								Count: out[m.Name].Count + dp.Value,
							}
						}
					}
					if len(out) != 0 {
						b, _ := json.Marshal(out)
						fmt.Fprintf(os.Stdout, "%s\n", b)
					}
				}
			}
		}()
	}

	apiHandler := http.Handler(api.NewAPIHandler(uid, clusterUUID, provider, expire, delay, percentDuplicate, percentTooMany, percentNonIndex, percentTooLarge, historyCap))
	if verbose {
		apiHandler = loggingMiddleware(apiHandler)
	}
	mux.Handle("/", apiHandler)

	switch {
	case certFile != "" && keyFile != "":
		if verbose {
			log.Printf("Starting HTTPS server on %s", addr)
		}
		if err := http.ListenAndServeTLS(addr, certFile, keyFile, mux); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("error running HTTPs server: %s", err)
			}
		}
	default:
		if verbose {
			log.Printf("Starting HTTP server on %s", addr)
		}
		if err := http.ListenAndServe(addr, mux); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("error running HTTP server: %s", err)
			}
		}
	}
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("error reading request body: %s", err)
			http.Error(w, "error reading request body", http.StatusInternalServerError)
			return
		}

		var body string
		if strings.EqualFold(r.Header.Get("content-encoding"), "gzip") {
			bodyReader, err := gzip.NewReader(bytes.NewReader(rawBody))
			if err != nil {
				log.Printf("cannot read gziped request body: %s", err)
				body = "<error reading request body>"
			}
			defer bodyReader.Close()
			bodyBytes, err := io.ReadAll(bodyReader)
			if err != nil {
				log.Printf("cannot read gziped body: %s", err)
				body = "<error reading gziped body>"
			}
			body = string(bodyBytes)
		}

		log.Printf("%s %s\n%s", r.Method, r.URL.RequestURI(), body)

		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewBuffer(rawBody))
		next.ServeHTTP(w, r)
	})
}
