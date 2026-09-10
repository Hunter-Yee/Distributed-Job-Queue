package db

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

//go:embed schema.sql
var schemaSQL string

// InitDB connects to PostgreSQL and runs automatic schema migrations.
func InitDB(connStr string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	// Retry connection for up to 30 seconds to handle DB startup in docker-compose
	for i := 1; i <= 15; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil {
			err = db.Ping()
			if err == nil {
				log.Printf("Successfully connected to PostgreSQL database")
				break
			}
		}
		log.Printf("Database connection attempt %d failed: %v. Retrying in 2 seconds...", i, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database after retries: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Execute migration
	if _, err := db.Exec(schemaSQL); err != nil {
		return nil, fmt.Errorf("failed to execute schema migration: %w", err)
	}

	log.Println("Database schema migrated successfully")
	return db, nil
}
