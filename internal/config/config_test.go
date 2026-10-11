package config_test

import (
	"os"
	"testing"

	"distributed-job-queue/internal/config"
)

func TestLoadConfig_Defaults(t *testing.T) {
	// Clear any environment variables that might affect the test
	keys := []string{
		"PORT", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSLMODE",
		"RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_USER", "RABBITMQ_PASSWORD",
	}
	for _, key := range keys {
		os.Unsetenv(key)
	}

	cfg := config.LoadConfig()

	if cfg.Port != "8080" {
		t.Errorf("expected default Port 8080, got %s", cfg.Port)
	}
	if cfg.DBHost != "db" {
		t.Errorf("expected default DBHost db, got %s", cfg.DBHost)
	}
	if cfg.RabbitMQHost != "localhost" {
		t.Errorf("expected default RabbitMQHost localhost, got %s", cfg.RabbitMQHost)
	}
	if cfg.RabbitMQPort != "5672" {
		t.Errorf("expected default RabbitMQPort 5672, got %s", cfg.RabbitMQPort)
	}
	if cfg.RabbitMQUser != "guest" {
		t.Errorf("expected default RabbitMQUser guest, got %s", cfg.RabbitMQUser)
	}
	if cfg.RabbitMQPassword != "guest" {
		t.Errorf("expected default RabbitMQPassword guest, got %s", cfg.RabbitMQPassword)
	}

	expectedRabbitURL := "amqp://guest:guest@localhost:5672/"
	if cfg.RabbitMQURL() != expectedRabbitURL {
		t.Errorf("expected RabbitMQURL %s, got %s", expectedRabbitURL, cfg.RabbitMQURL())
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	os.Setenv("PORT", "9090")
	os.Setenv("RABBITMQ_HOST", "rabbit.prod.local")
	os.Setenv("RABBITMQ_PORT", "5673")
	os.Setenv("RABBITMQ_USER", "admin")
	os.Setenv("RABBITMQ_PASSWORD", "secret123")

	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("RABBITMQ_HOST")
		os.Unsetenv("RABBITMQ_PORT")
		os.Unsetenv("RABBITMQ_USER")
		os.Unsetenv("RABBITMQ_PASSWORD")
	}()

	cfg := config.LoadConfig()

	if cfg.Port != "9090" {
		t.Errorf("expected Port 9090, got %s", cfg.Port)
	}
	if cfg.RabbitMQHost != "rabbit.prod.local" {
		t.Errorf("expected RabbitMQHost rabbit.prod.local, got %s", cfg.RabbitMQHost)
	}
	if cfg.RabbitMQPort != "5673" {
		t.Errorf("expected RabbitMQPort 5673, got %s", cfg.RabbitMQPort)
	}
	if cfg.RabbitMQUser != "admin" {
		t.Errorf("expected RabbitMQUser admin, got %s", cfg.RabbitMQUser)
	}
	if cfg.RabbitMQPassword != "secret123" {
		t.Errorf("expected RabbitMQPassword secret123, got %s", cfg.RabbitMQPassword)
	}

	expectedRabbitURL := "amqp://admin:secret123@rabbit.prod.local:5673/"
	if cfg.RabbitMQURL() != expectedRabbitURL {
		t.Errorf("expected RabbitMQURL %s, got %s", expectedRabbitURL, cfg.RabbitMQURL())
	}
}
