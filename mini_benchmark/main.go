package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Configuration
type Config struct {
	// Server URLs
	CreateRoomURL  string
	SendMessageURL string
	ReadMessageURL string

	// Test parameters
	NumGoroutines        int
	MessagesPerGoroutine int
	MessageSizeKB        int
	TestMode             string // "write", "read", "both"

	// Data
	User  string
	Rooms []string
}

// Request/Response structures
type CreateRoomRequest struct {
	Name         string   `json:"name"`
	AllowedUsers []string `json:"allowed_users"`
}

type SendMessageRequest struct {
	User string `json:"user"`
	Room string `json:"room"`
	Msg  string `json:"msg"`
}

// Results tracking
type TestResults struct {
	mu                 sync.Mutex
	TotalRequests      int
	SuccessfulRequests int
	FailedRequests     int
	TotalDataKB        int64
	StartTime          time.Time
	EndTime            time.Time
	ErrorCounts        map[int]int // HTTP status code -> count
}

func (r *TestResults) AddResult(success bool, statusCode int, dataKB int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.TotalRequests++
	r.TotalDataKB += dataKB

	if success {
		r.SuccessfulRequests++
	} else {
		r.FailedRequests++
		if r.ErrorCounts == nil {
			r.ErrorCounts = make(map[int]int)
		}
		r.ErrorCounts[statusCode]++
	}
}

func (r *TestResults) PrintResults(testType string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	duration := r.EndTime.Sub(r.StartTime)
	rps := float64(r.TotalRequests) / duration.Seconds()
	successRate := float64(r.SuccessfulRequests) / float64(r.TotalRequests) * 100
	throughputMBps := float64(r.TotalDataKB) / 1024 / duration.Seconds()

	fmt.Printf("\n===============================================\n")
	fmt.Printf("%s TEST RESULTS\n", strings.ToUpper(testType))
	fmt.Printf("===============================================\n")
	fmt.Printf("Total requests: %d\n", r.TotalRequests)
	fmt.Printf("Successful requests: %d\n", r.SuccessfulRequests)
	fmt.Printf("Failed requests: %d\n", r.FailedRequests)
	fmt.Printf("Success rate: %.2f%%\n", successRate)
	fmt.Printf("Duration: %.2fs\n", duration.Seconds())
	fmt.Printf("Requests per second: %.2f\n", rps)
	fmt.Printf("Data processed: %.2f MB\n", float64(r.TotalDataKB)/1024)
	fmt.Printf("Throughput: %.2f MB/s\n", throughputMBps)

	if len(r.ErrorCounts) > 0 {
		fmt.Printf("Error breakdown:\n")
		for code, count := range r.ErrorCounts {
			fmt.Printf("  HTTP %d: %d requests\n", code, count)
		}
	}
	fmt.Printf("===============================================\n")
}

func main() {
	// Command line flags
	var (
		numGoroutines        = flag.Int("goroutines", 10, "Number of concurrent goroutines")
		messagesPerGoroutine = flag.Int("messages", 100, "Number of messages per goroutine")
		messageSizeKB        = flag.Int("size", 32, "Message size in KB")
		testMode             = flag.String("mode", "both", "Test mode: write, read, or both")
		createURL            = flag.String("create-url", "http://localhost:80/v1/addroom", "Create room URL")
		sendURL              = flag.String("send-url", "http://localhost:80/v1/sendmessage", "Send message URL")
		readURL              = flag.String("read-url", "http://localhost:80/v1/post", "Read message URL")
		user                 = flag.String("user", "Leon", "Username for testing")
	)
	flag.Parse()

	config := Config{
		CreateRoomURL:        *createURL,
		SendMessageURL:       *sendURL,
		ReadMessageURL:       *readURL,
		NumGoroutines:        *numGoroutines,
		MessagesPerGoroutine: *messagesPerGoroutine,
		MessageSizeKB:        *messageSizeKB,
		TestMode:             *testMode,
		User:                 *user,
		Rooms:                []string{"alpha", "bravo", "golf", "hotel", "mike", "november", "sierra", "tango"},
	}

	fmt.Printf("===============================================\n")
	fmt.Printf("LOAD TEST CONFIGURATION\n")
	fmt.Printf("===============================================\n")
	fmt.Printf("Goroutines: %d\n", config.NumGoroutines)
	fmt.Printf("Messages per goroutine: %d\n", config.MessagesPerGoroutine)
	fmt.Printf("Message size: %d KB\n", config.MessageSizeKB)
	fmt.Printf("Test mode: %s\n", config.TestMode)
	fmt.Printf("Total operations: %d\n", config.NumGoroutines*config.MessagesPerGoroutine)
	fmt.Printf("Expected data: %.2f MB\n", float64(config.NumGoroutines*config.MessagesPerGoroutine*config.MessageSizeKB)/1024)
	fmt.Printf("User: %s\n", config.User)
	fmt.Printf("Rooms: %v\n", config.Rooms)
	fmt.Printf("===============================================\n")

	// Create rooms first
	if config.TestMode == "write" || config.TestMode == "both" {
		fmt.Printf("Creating rooms...\n")
		createRooms(config)
		time.Sleep(1 * time.Second) // Give server time to process
	}

	// Run tests
	if config.TestMode == "write" || config.TestMode == "both" {
		fmt.Printf("Starting write test...\n")
		runWriteTest(config)
	}

	if config.TestMode == "read" || config.TestMode == "both" {
		fmt.Printf("Starting read test...\n")
		runReadTest(config)
	}
}

func createRooms(config Config) {
	client := &http.Client{Timeout: 10 * time.Second}

	for _, room := range config.Rooms {
		reqBody := CreateRoomRequest{
			Name:         room,
			AllowedUsers: []string{config.User},
		}

		jsonData, _ := json.Marshal(reqBody)

		resp, err := client.Post(config.CreateRoomURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error creating room %s: %v\n", room, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == 200 || resp.StatusCode == 201 {
			fmt.Printf("✓ Room %s created\n", room)
		} else {
			fmt.Printf("✗ Room %s failed (HTTP %d)\n", room, resp.StatusCode)
		}
	}
}

func runWriteTest(config Config) {
	results := &TestResults{
		StartTime: time.Now(),
	}

	// Generate message content once
	messageContent := strings.Repeat("X", config.MessageSizeKB*1024)

	var wg sync.WaitGroup

	for i := 0; i < config.NumGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			writeWorker(goroutineID, config, results, messageContent)
		}(i)
	}

	wg.Wait()
	results.EndTime = time.Now()

	results.PrintResults("WRITE")
}

func writeWorker(goroutineID int, config Config, results *TestResults, messageContent string) {
	client := &http.Client{Timeout: 30 * time.Second}

	for i := 0; i < config.MessagesPerGoroutine; i++ {
		// Round-robin room selection
		room := config.Rooms[(goroutineID*config.MessagesPerGoroutine+i)%len(config.Rooms)]

		msg := fmt.Sprintf("[G%d-M%d in %s] %s", goroutineID, i, room, messageContent)

		reqBody := SendMessageRequest{
			User: config.User,
			Room: room,
			Msg:  msg,
		}

		jsonData, _ := json.Marshal(reqBody)

		resp, err := client.Post(config.SendMessageURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			results.AddResult(false, 0, 0)
			continue
		}

		success := resp.StatusCode == 200 || resp.StatusCode == 201
		dataKB := int64(len(jsonData)) / 1024
		results.AddResult(success, resp.StatusCode, dataKB)

		resp.Body.Close()
	}
}

func runReadTest(config Config) {
	results := &TestResults{
		StartTime: time.Now(),
	}

	var wg sync.WaitGroup

	for i := 0; i < config.NumGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			readWorker(goroutineID, config, results)
		}(i)
	}

	wg.Wait()
	results.EndTime = time.Now()

	results.PrintResults("READ")
}

func readWorker(goroutineID int, config Config, results *TestResults) {
	client := &http.Client{Timeout: 30 * time.Second}

	for i := 0; i < config.MessagesPerGoroutine; i++ {
		// Round-robin room selection
		room := config.Rooms[(goroutineID*config.MessagesPerGoroutine+i)%len(config.Rooms)]

		// Request a reasonable number of messages (adjust as needed)
		messagesPerRequest := 4
		url := fmt.Sprintf("%s/%s?n=%d", config.ReadMessageURL, room, messagesPerRequest)

		resp, err := client.Get(url)
		if err != nil {
			results.AddResult(false, 0, 0)
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			results.AddResult(false, resp.StatusCode, 0)
			continue
		}

		success := resp.StatusCode == 200
		dataKB := int64(len(bodyBytes)) / 1024
		results.AddResult(success, resp.StatusCode, dataKB)
	}
}
