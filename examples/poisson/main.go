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
	call := flag.String("call", "helloworld.Greeter.SayHello", "fully-qualified method to call")
	proto := flag.String("proto", "testdata/greeter.proto", "path to .proto file describing the service")
	data := flag.String("d", "{\"endpoint\":\"endpoint1\"}", "The call data as stringified JSON.")
	rps := flag.Uint64("rps", 50, "mean request rate (lambda) for the Poisson arrival process")
	z := flag.Duration("duration", 30, "total number of requests to send")
	m := flag.String("m", "{}", "metadata as stringified JSON")
	nConns := flag.Uint("connections", 1, "number of gRPC connections to use")
	c := flag.Uint("concurrency", 50, "number of concurrent workers")
	insecure := flag.Bool("insecure", true, "use an insecure (non-TLS) connection")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <host:port>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	target := flag.Arg(0)

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
		runner.WithDataFromJSON(*data),
		runner.WithInsecure(*insecure),
		runner.WithConcurrency(*c),
		runner.WithRunDuration(*z),
		runner.WithMetadataFromJSON(*m),
		runner.WithConnections(*nConns),
		// Async dispatch is required for genuine Poisson (open-loop) arrivals:
		// without it, each worker blocks on the previous request's completion
		// before accepting its next scheduled tick, which distorts the
		// inter-arrival distribution whenever the server is slower than the
		// scheduled rate.
		runner.WithAsync(true),
		// A custom pacer bypasses runner's own n->Max wiring (that only
		// happens for the built-in schedules in createPacer), so Max must be
		// set here directly or the run never stops on request count.
		runner.WithPacer(&load.PoissonPacer{Freq: *rps, Max: uint64(0)}),
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
