package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type workerResult struct {
	latencies []time.Duration
	errors    int
}

func main() {
	addr := flag.String("addr", "localhost:6379", "Target godis-kv server address")
	workers := flag.Int("workers", 50, "Number of concurrent connections (workers)")
	requests := flag.Int("requests", 10000, "Total number of requests to execute")
	mode := flag.String("mode", "mixed", "Workload mode: mixed (50% SET / 50% GET), set, or get")
	flag.Parse()

	if *workers <= 0 || *requests <= 0 {
		fmt.Fprintln(os.Stderr, "ERR: -workers and -requests must be greater than zero")
		os.Exit(1)
	}

	opMode := strings.ToLower(*mode)
	if opMode != "mixed" && opMode != "set" && opMode != "get" {
		fmt.Fprintln(os.Stderr, "ERR: invalid -mode (use: mixed, set, or get)")
		os.Exit(1)
	}

	numWorkers := min(*workers, *requests)
	baseReqs := *requests / numWorkers
	remainder := *requests % numWorkers

	results := make([]workerResult, numWorkers)
	var wg sync.WaitGroup

	fmt.Printf("Starting benchmark on %s | workers=%d | requests=%d | mode=%s\n",
		*addr, numWorkers, *requests, opMode)

	startTotal := time.Now()

	for w := 0; w < numWorkers; w++ {
		count := baseReqs
		if w < remainder {
			count++
		}

		wg.Add(1)
		go func(workerID, total int) {
			defer wg.Done()
			results[workerID] = runWorker(*addr, workerID, total, opMode)
		}(w, count)
	}

	wg.Wait()
	totalElapsed := time.Since(startTotal)

	printReport(*addr, numWorkers, opMode, totalElapsed, results)
}

func runWorker(addr string, workerID, count int, mode string) workerResult {
	res := workerResult{
		latencies: make([]time.Duration, 0, count),
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		res.errors = count
		return res
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for i := 0; i < count; i++ {
		cmd := buildCommand(workerID, i, mode)

		reqStart := time.Now()
		if _, err := conn.Write([]byte(cmd)); err != nil {
			res.errors += count - i
			return res
		}

		resp, err := reader.ReadString('\n')
		if err != nil {
			res.errors += count - i
			return res
		}

		if strings.HasPrefix(resp, "ERR") {
			res.errors++
			continue
		}

		res.latencies = append(res.latencies, time.Since(reqStart))
	}

	return res
}

func buildCommand(workerID, seq int, mode string) string {
	switch mode {
	case "set":
		return fmt.Sprintf("SET bench:%d:%d payload_%d\n", workerID, seq, seq)
	case "get":
		return fmt.Sprintf("GET bench:%d:%d\n", workerID, seq)
	default:
		keyID := seq / 2
		if seq%2 == 0 {
			return fmt.Sprintf("SET bench:%d:%d payload_%d\n", workerID, keyID, keyID)
		}
		return fmt.Sprintf("GET bench:%d:%d\n", workerID, keyID)
	}
}

func printReport(addr string, workers int, mode string, elapsed time.Duration, results []workerResult) {
	var (
		allLatencies []time.Duration
		totalErrors  int
		totalSum     time.Duration
	)

	for _, r := range results {
		totalErrors += r.errors
		allLatencies = append(allLatencies, r.latencies...)
		for _, l := range r.latencies {
			totalSum += l
		}
	}

	succeeded := len(allLatencies)
	if succeeded == 0 {
		fmt.Printf("\nNo requests completed successfully (errors=%d). Check if the server is running on %s.\n", totalErrors, addr)
		return
	}

	slices.Sort(allLatencies)

	throughput := float64(succeeded) / elapsed.Seconds()
	avg := totalSum / time.Duration(succeeded)
	minLat := allLatencies[0]
	maxLat := allLatencies[succeeded-1]
	p50 := percentile(allLatencies, 50)
	p95 := percentile(allLatencies, 95)
	p99 := percentile(allLatencies, 99)

	fmt.Println("\n========== BENCHMARK RESULTS ==========")
	fmt.Printf("Target:          %s (%s)\n", addr, mode)
	fmt.Printf("Workers:         %d\n", workers)
	fmt.Printf("Completed:       %d (errors: %d)\n", succeeded, totalErrors)
	fmt.Printf("Total time:      %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput:      %.2f req/s\n", throughput)
	fmt.Println("---------------------------------------")
	fmt.Printf("Min latency:     %v\n", minLat)
	fmt.Printf("Avg latency:     %v\n", avg)
	fmt.Printf("P50 latency:     %v\n", p50)
	fmt.Printf("P95 latency:     %v\n", p95)
	fmt.Printf("P99 latency:     %v\n", p99)
	fmt.Printf("Max latency:     %v\n", maxLat)
	fmt.Println("=======================================")
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil((p / 100.0) * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}