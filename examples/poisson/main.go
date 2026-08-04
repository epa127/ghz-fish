// Command poisson runs a gRPC load test whose requests are dispatched
// according to a Poisson arrival process (exponentially distributed
// inter-arrival times) instead of ghz's usual constant/step/line rate
// schedules. It drives the runner package directly via runner.WithPacer,
// since the ghz CLI binary does not expose custom pacers.
//
// If -host is not given, an in-process test gRPC server is started
// automatically so the example runs standalone with `go run`.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/bojand/ghz/internal"
	"github.com/bojand/ghz/load"
	"github.com/bojand/ghz/printer"
	"github.com/bojand/ghz/runner"
)

func main() {
	host := flag.String("host", "", "gRPC host:port to target; if empty, an in-process test server is started")
	call := flag.String("call", "helloworld.Greeter.SayHello", "fully-qualified method to call")
	proto := flag.String("proto", "testdata/greeter.proto", "path to .proto file describing the service")
	dataFile := flag.String("data", "testdata/data.json", "path to JSON request data file")
	rps := flag.Uint64("rps", 50, "mean request rate (lambda) for the Poisson arrival process")
	n := flag.Uint("n", 2000, "total number of requests to send")
	c := flag.Uint("c", 50, "number of concurrent workers")
	insecure := flag.Bool("insecure", true, "use an insecure (non-TLS) connection")
	flag.Parse()

	target := *host
	if target == "" {
		_, server, err := internal.StartServer(false)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to start in-process test server:", err)
			os.Exit(1)
		}
		defer server.GracefulStop()

		target = internal.TestLocalhost
	}

	report, err := runner.Run(
		*call,
		target,
		runner.WithProtoFile(*proto, []string{}),
		runner.WithDataFromFile(*dataFile),
		runner.WithInsecure(*insecure),
		runner.WithConcurrency(*c),
		runner.WithTotalRequests(*n),
		// Async dispatch is required for genuine Poisson (open-loop) arrivals:
		// without it, each worker blocks on the previous request's completion
		// before accepting its next scheduled tick, which distorts the
		// inter-arrival distribution whenever the server is slower than the
		// scheduled rate.
		runner.WithAsync(true),
		// A custom pacer bypasses runner's own n->Max wiring (that only
		// happens for the built-in schedules in createPacer), so Max must be
		// set here directly or the run never stops on request count.
		runner.WithPacer(&load.PoissonPacer{Freq: *rps, Max: uint64(*n)}),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	p := printer.ReportPrinter{Out: os.Stdout, Report: report}
	if err := p.Print("pretty"); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
