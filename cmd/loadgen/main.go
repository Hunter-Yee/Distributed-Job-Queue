package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"distributed-job-queue/internal/models"
)

type Config struct {
	APIURL      string
	TotalJobs   int
	Concurrency int
	FailureRate float64
	DurationMS  int
}

type Result struct {
	Duration time.Duration
	Success  bool
	Error    error
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.APIURL, "url", "http://localhost:8080/jobs", "Target API URL for job creation")
	flag.IntVar(&cfg.TotalJobs, "jobs", 1000, "Total number of jobs to dispatch")
	flag.IntVar(&cfg.Concurrency, "concurrency", 50, "Number of concurrent job dispatch workers")
	flag.Float64Var(&cfg.FailureRate, "failure-rate", 0.05, "Simulated execution failure probability (0.0 to 1.0)")
	flag.IntVar(&cfg.DurationMS, "duration-ms", 150, "Simulated workload execution duration in ms")
	flag.Parse()

	fmt.Println("=========================================================")
	fmt.Println("       Distributed Job Queue - Load Generator & Benchmark")
	fmt.Println("=========================================================")
	fmt.Printf("API Target:       %s\n", cfg.APIURL)
	fmt.Printf("Total Jobs:       %d\n", cfg.TotalJobs)
	fmt.Printf("Concurrency:      %d clients\n", cfg.Concurrency)
	fmt.Printf("Workload Duration: %d ms\n", cfg.DurationMS)
	fmt.Printf("Simulated Fail:   %.2f%%\n", cfg.FailureRate*100)
	fmt.Println("=========================================================")

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency * 2,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	jobTypes := []models.JobType{
		models.JobTypeCompute,
		models.JobTypeEmail,
		models.JobTypeReport,
		models.JobTypeImageProcessing,
	}

	priorities := []models.JobPriority{
		models.PriorityLow,
		models.PriorityMedium,
		models.PriorityHigh,
		models.PriorityCritical,
	}

	jobQueue := make(chan int, cfg.TotalJobs)
	for i := 1; i <= cfg.TotalJobs; i++ {
		jobQueue <- i
	}
	close(jobQueue)

	results := make([]Result, cfg.TotalJobs)
	var wg sync.WaitGroup
	var completedCount atomic.Int64
	var successCount atomic.Int64
	var errorCount atomic.Int64

	startTime := time.Now()

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

			for jobIndex := range jobQueue {
				jType := jobTypes[rng.Intn(len(jobTypes))]
				jPriority := priorities[rng.Intn(len(priorities))]

				reqPayload := models.CreateJobRequest{
					Type:     string(jType),
					Priority: string(jPriority),
					Payload: &models.JobPayload{
						DurationMS:         cfg.DurationMS,
						FailureProbability: cfg.FailureRate,
					},
				}

				bodyBytes, _ := json.Marshal(reqPayload)

				reqStart := time.Now()
				req, err := http.NewRequest(http.MethodPost, cfg.APIURL, bytes.NewReader(bodyBytes))
				if err != nil {
					results[jobIndex-1] = Result{Duration: time.Since(reqStart), Success: false, Error: err}
					errorCount.Add(1)
					completedCount.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				reqDuration := time.Since(reqStart)

				if err != nil {
					results[jobIndex-1] = Result{Duration: reqDuration, Success: false, Error: err}
					errorCount.Add(1)
				} else {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode == http.StatusCreated {
						results[jobIndex-1] = Result{Duration: reqDuration, Success: true}
						successCount.Add(1)
					} else {
						results[jobIndex-1] = Result{Duration: reqDuration, Success: false, Error: fmt.Errorf("status %d", resp.StatusCode)}
						errorCount.Add(1)
					}
				}

				done := completedCount.Add(1)
				if done%100 == 0 || done == int64(cfg.TotalJobs) {
					fmt.Printf("\rProgress: %d / %d jobs sent (%.1f%%)", done, cfg.TotalJobs, float64(done)/float64(cfg.TotalJobs)*100)
				}
			}
		}()
	}

	wg.Wait()
	totalElapsed := time.Since(startTime)
	fmt.Printf("\n\nFinished in %v\n", totalElapsed)

	// Calculate and report metrics
	var durations []time.Duration
	for _, res := range results {
		durations = append(durations, res.Duration)
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	rps := float64(cfg.TotalJobs) / totalElapsed.Seconds()
	p50 := durations[len(durations)*50/100]
	p95 := durations[len(durations)*95/100]
	p99 := durations[len(durations)*99/100]
	maxLatency := durations[len(durations)-1]
	minLatency := durations[0]

	fmt.Println("=========================================================")
	fmt.Println("                    BENCHMARK RESULTS                    ")
	fmt.Println("=========================================================")
	fmt.Printf("Total Dispatched:  %d\n", cfg.TotalJobs)
	fmt.Printf("Successful (201):  %d (%.2f%%)\n", successCount.Load(), float64(successCount.Load())/float64(cfg.TotalJobs)*100)
	fmt.Printf("Failed:            %d\n", errorCount.Load())
	fmt.Printf("Ingestion Rate:    %.2f jobs/sec (RPS)\n", rps)
	fmt.Println("---------------------------------------------------------")
	fmt.Println("Latency Percentiles (API Ingestion Roundtrip):")
	fmt.Printf("  Min:   %v\n", minLatency)
	fmt.Printf("  p50:   %v\n", p50)
	fmt.Printf("  p95:   %v\n", p95)
	fmt.Printf("  p99:   %v\n", p99)
	fmt.Printf("  Max:   %v\n", maxLatency)
	fmt.Println("=========================================================")
}
