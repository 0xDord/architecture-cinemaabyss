package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
)

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {

	http.HandleFunc("/api/movies", handleMovies)
	http.HandleFunc("/api/users", handleUsers)
	http.HandleFunc("/health", handleHealth)

	port := getEnv("PORT", "8000")

	log.Printf("Proxy dd service listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleMovies(w http.ResponseWriter, r *http.Request) {
	var targetURL string
	gradualMigration := getEnv("GRADUAL_MIGRATION", "false")
	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "0")
	monolithURL := getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")

	migrationPercent, err := strconv.Atoi(migrationPercentStr)
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT: %s, defaulting to 0", migrationPercentStr)
		migrationPercent = 0
	}

	if gradualMigration == "true" {
		if rand.Intn(100) < migrationPercent {
			targetURL = moviesServiceURL + r.URL.Path
		} else {
			targetURL = monolithURL + r.URL.Path
		}
	} else {
		targetURL = monolithURL + r.URL.Path
	}

	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to create proxy request"))
		return
	}
	proxyReq.Header = r.Header.Clone()

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("Failed to reach backend service"))
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func handleUsers(w http.ResponseWriter, r *http.Request) {
	monolithURL := getEnv("MONOLITH_URL", "http://localhost:8080")
	targetURL := monolithURL + r.URL.Path

	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to create proxy request"))
		return
	}
	proxyReq.Header = r.Header.Clone()

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("Failed to reach backend service"))
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}
