# mock-es

## What is it

An API and CLI application for running a mock elasticsearch server.  The server implements the absoulte minimum for something like `filebeat` to connect to it and send bulk requests.  The data that is sent is thrown away.  The server can be configured to send error responses but by default all actions succeed.

## Use cases

If you are developing Dashboards for Elastic Agent and want to make sure that the errors Dashboards display correctly you could start `mock-es` configured to return errors and then direct the integrations to send data to `mock-es`.  This would allow you to prove that when errors occur the Dashboards are populated correctly.

You are developing a feature that splits the batch when `StatusEntityTooLarge` is returned.  You could write a unit test that start the server from the API with a 100% chance of returning that error, then send data, making sure that the split happens as expected and checking the metrics that `StatusEntityTooLarge` was returned.


## Building the CLI server

```bash
go install github.com/elastic/mock-es/cmd/mock-es@latest
```

## Running the CLI server

To run the server with defaults (port 9200, no TLS, always succeed).  Simply run the executable:

```
mock-es
```

Options are used to change the behavior.

### General options

| Flag                | Meaning                                                                                       |
|---------------------|-----------------------------------------------------------------------------------------------|
| -addr string        | address to listen on ip:port (default ":9200")                                                |
| -clusteruuid string | Cluster UUID of Elasticsearch we are mocking, needed if beat is being monitored by metricbeat |
| -metrics duration   | Go 'time.Duration' to wait between printing metrics to stdout, 0 is no metrics                |
| -delay duration     | Go 'time.Duration' to wait before processing API request, 0 is no delay                       |


### TLS Options

Both `certfile` and `keyfile` are needed to enable TLS

| Flag             | Meaning                                             |
|------------------|-----------------------------------------------------|
| -certfile string | path to PEM certificate file, empty sting is no TLS |
| -keyfile string  | path to PEM private key file, empty sting is no TLS |


### Error Option

| Flag           | Meaning                                                                           |
|----------------|-----------------------------------------------------------------------------------|
| -toolarge uint | percent chance StatusEntityTooLarge is returned for POST method on _bulk endpoint |
| -dup uint      | percent chance StatusConflict is returned for create action                       |
| -nonindex uint | percent chance StatusNotAcceptable is returned for create action                  |
| -toomany uint  | percent chance StatusTooManyRequests is returned for create action                |


`-toolarge` will be for the entire POST to the _bulk endpoint.  The others are for each individual create action in the bulk request.  `-toolarge` cannot be larger than 100.  The sum of `-dup`, `-noindex`, and `-toomany` cannot be larger than 100.

#### Example

```
./mock-es -toolarge 20 -dup 5 -nonindex 10 -toomany 15
```

This means there is a 20% chance the POST to _bulk will return StatusEntityTooLarge, and an 80% chance it will succeed.  There is a 5% chance that the create action will return StatusConflict (duplicate entry), a 10% chance that the create action will return StatusNotAcceptable (non index) and a 15% chance that the create action will return StatusTooManyRequests.


## Using in a Unit Test

Rather than trying to build and shell out to run the `mock-es` executable it is much easier to just create the server in your tests.  A minimal example would be:

``` go
import (
	"net/http"
	"time"

	"github.com/elastic/mock-es/pkg/api"
	"github.com/gofrs/uuid/v5"
)

func main() {
	mux := http.NewServeMux()
	mux.Handle("/", api.NewAPIHandler(uuid.Must(uuid.NewV4()), "", nil, time.Now().Add(24 *time.Hour) , 0, 0, 0, 0))
	if err := http.ListenAndServe("localhost:9200", mux); err != nil {
		if err != http.ErrServerClosed {
			panic(err)
		}
	}
}
```

## Using the Deterministic Handler in a Unit Test

For more predictable behavior in unit tests, especially when you need to control
the exact responses for specific actions, you can use the 
`NewDeterministicAPIHandler`. This handler takes a function that you define, 
which will be called for each action in a bulk request. Your function receives 
the action details and the event payload, and it should return the desired HTTP
status code for that action.

Here's a minimal example:

```go
import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/elastic/mock-es/pkg/api"
	"github.com/gofrs/uuid/v5"
)

func main() {
	deterministicHandler := func(action api.Action, event []byte) int {
		// Example: Always return 200 OK for 'create' actions
		if action.Action == "create" {
			return http.StatusOK
		}
		// Example: Return 409 Conflict for 'index' actions with a specific ID
		if action.Action == "index" {
			var meta struct {
				ID string `json:"_id"`
			}
			if err := json.Unmarshal(action.Meta, &meta); err == nil {
				if meta.ID == "specific-id-to-fail" {
					return http.StatusConflict
				}
			}
		}
		// Default for other actions or unhandled cases
		return http.StatusOK
	}

	mux := http.NewServeMux()
	mux.Handle("/", api.NewDeterministicAPIHandler(
		uuid.Must(uuid.NewV4()),
		"",
		nil,
		time.Now().Add(24*time.Hour),
		0,
		0,
		deterministicHandler,
	))

	if err := http.ListenAndServe("localhost:9200", mux); err != nil {
		if err != http.ErrServerClosed {
			panic(err)
		}
	}
}
```

## Reading metrics

``` go
import (
	"net/http"
	"time"

	"github.com/elastic/mock-es/pkg/api"
	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func main() {
	rdr := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

	mux := http.NewServeMux()
	mux.Handle("/", api.NewAPIHandler(uuid.Must(uuid.NewV4()), "", provider, time.Now().Add(24 *time.Hour) , 0, 0, 0, 0))

	go func() {
		if err := http.ListenAndServe("localhost:9200", mux); err != nil {
			if err != http.ErrServerClosed {
				panic(err)
			}
		}
	}()

	// send requests
	// ...

	// read metrics
	rm := metricdata.ResourceMetrics{}
	if err := rdr.Collect(context.Background(), &rm); err != nil {
		panic(err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				// check
			}
		}
	}
}
```
