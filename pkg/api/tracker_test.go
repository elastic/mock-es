package api

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/require"
)

func TestUserAgentTracker(t *testing.T) {
	isStarted := sync.WaitGroup{}
	isStarted.Add(1)
	handler := NewAPIHandler(uuid.New(), "", metrics.DefaultRegistry, time.Now().Add(24*time.Hour), 0, 0, 0, 0, 0)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/", handler)
		isStarted.Done()
		if err := http.ListenAndServe(":9200", mux); err != nil {
			if err != http.ErrServerClosed {
				require.NoError(t, err)
				isStarted.Done()
			}
		}
	}()

	isStarted.Wait()

	getCount := 5

	writers := sync.WaitGroup{}
	writers.Add(getCount * 2)
	go func() {
		for i := 0; i < getCount; i++ {
			client := &http.Client{Timeout: time.Second * 10}
			req, err := http.NewRequest("GET", "http://localhost:9200/", nil)
			require.NoError(t, err)

			req.Header.Set("User-Agent", "root-get")
			_, err = client.Do(req)
			require.NoError(t, err)
			writers.Done()
		}
	}()

	go func() {
		for i := 0; i < getCount; i++ {
			client := &http.Client{Timeout: time.Second * 10}
			req, err := http.NewRequest("GET", "http://localhost:9200/_license", nil)
			require.NoError(t, err)

			req.Header.Set("User-Agent", "license-get")
			_, err = client.Do(req)
			require.NoError(t, err)
			writers.Done()
		}
	}()

	writers.Wait()

	data := handler.UserAgents.Get()
	expected := UserAgentMaps{root: map[string]int{"root-get": 5}, license: map[string]int{"license-get": 5}, bulk: map[string]int{}}
	require.Equal(t, expected, data)
}
