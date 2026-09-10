package api

import (
	"database/sql"
	"net/http"

	"distributed-job-queue/internal/repository"
)

func NewRouter(db *sql.DB) http.Handler {
	mux := http.NewServeMux()

	repo := repository.NewPostgresJobRepository(db)
	jobHandler := NewJobHandler(repo)

	// Register API endpoints
	mux.HandleFunc("POST /jobs", jobHandler.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", jobHandler.GetJobByID)
	mux.HandleFunc("GET /jobs", jobHandler.ListJobs)

	// Health check endpoint
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(); err != nil {
			writeJSONError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	return mux
}
