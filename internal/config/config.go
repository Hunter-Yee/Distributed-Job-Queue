package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port             string
	DBHost           string
	DBPort           string
	DBUser           string
	DBPassword       string
	DBName           string
	DBSSLMode        string
	RabbitMQHost     string
	RabbitMQPort     string
	RabbitMQUser     string
	RabbitMQPassword string
	WorkerConcurrency int
	WorkerPrefetch    int
	MaxRetries        int
	BaseRetryDelayMS  int
}

func LoadConfig() *Config {
	concurrency := 5
	if cStr := os.Getenv("WORKER_CONCURRENCY"); cStr != "" {
		if val, err := strconv.Atoi(cStr); err == nil && val > 0 {
			concurrency = val
		}
	}

	prefetch := 10
	if pStr := os.Getenv("WORKER_PREFETCH"); pStr != "" {
		if val, err := strconv.Atoi(pStr); err == nil && val > 0 {
			prefetch = val
		}
	}

	maxRetries := 3
	if rStr := os.Getenv("MAX_RETRIES"); rStr != "" {
		if val, err := strconv.Atoi(rStr); err == nil && val >= 0 {
			maxRetries = val
		}
	}

	baseRetryDelayMS := 1000 // 1 second
	if dStr := os.Getenv("BASE_RETRY_DELAY_MS"); dStr != "" {
		if val, err := strconv.Atoi(dStr); err == nil && val > 0 {
			baseRetryDelayMS = val
		}
	}

	return &Config{
		Port:              getEnv("PORT", "8080"),
		DBHost:            getEnv("DB_HOST", "db"),
		DBPort:            getEnv("DB_PORT", "5432"),
		DBUser:            getEnv("DB_USER", "postgres"),
		DBPassword:        getEnv("DB_PASSWORD", "postgres"),
		DBName:            getEnv("DB_NAME", "jobqueue"),
		DBSSLMode:         getEnv("DB_SSLMODE", "disable"),
		RabbitMQHost:      getEnv("RABBITMQ_HOST", "localhost"),
		RabbitMQPort:      getEnv("RABBITMQ_PORT", "5672"),
		RabbitMQUser:      getEnv("RABBITMQ_USER", "guest"),
		RabbitMQPassword:  getEnv("RABBITMQ_PASSWORD", "guest"),
		WorkerConcurrency: concurrency,
		WorkerPrefetch:    prefetch,
		MaxRetries:        maxRetries,
		BaseRetryDelayMS:  baseRetryDelayMS,
	}
}

func (c *Config) DBConnString() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName, c.DBSSLMode,
	)
}

func (c *Config) RabbitMQURL() string {
	return fmt.Sprintf(
		"amqp://%s:%s@%s:%s/",
		c.RabbitMQUser, c.RabbitMQPassword, c.RabbitMQHost, c.RabbitMQPort,
	)
}

func getEnv(key, defaultValue string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultValue
}
