# mock-es

## What is it

An API and CLI application for running a mock elasticsearch server.  The server implements the absoulte minimum for something like `filebeat` to connect to it and send bulk requests.  The data that is sent is thrown away.  The server can be configured to send error responses but by default all actions succeed.

## Use cases

If you are developing Dashboards for Elastic Agent and want to make sure that the errors Dashboards display correctly you could start `mock-es` configured to return errors and then direct the integrations to send data to `mock-es`.  This would allow you to prove that when errors occur the Dashboards are populated correctly.

You are developing a feature that splits the batch when `StatusEntityTooLarge` is returned.  You could write a unit test that start the server from the API with a 100% chance of returning that error, then send data, making sure that the split happens as expected and checking the metrics that `StatusEntityTooLarge` was returned.


## Building the CLI server

```
git clone https://github.com/elastic/mock-es.git
cd mock-es/cmd/mock-es
go build
```

## Running the CLI server

To run the server with defaults (port 9200, no TLS, always succeed).  Simply run the executable:

```
./mock-es
```

Options are used to change the behavior.

### General options

| Flag                | Meaning                                                                                       |
|---------------------|-----------------------------------------------------------------------------------------------|
| -addr string        | address to listen on ip:port (default ":9200")                                                |
| -clusteruuid string | Cluster UUID of Elasticsearch we are mocking, needed if beat is being monitored by metricbeat |
| -metrics duration   | Interval to print metrics to stdout, 0 is no metrics                                          |
| -delay duration     | Golang Duration to wait before processing API request, 0 is no delay                          |


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
	"github.com/google/uuid"
	"github.com/rcrowley/go-metrics"
)

func main() {
	mux := http.NewServeMux()
	mux.Handle("/", api.NewAPIHandler(uuid.New(), "", metrics.DefaultRegistry, time.Now().Add(24 *time.Hour) , 0, 0, 0, 0))
	if err := http.ListenAndServe("localhost:9200", mux); err != nil {
		if err != http.ErrServerClosed {
			panic(err)
		}
	}
}
```

