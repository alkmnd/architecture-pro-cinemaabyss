package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

var eventTopics = map[string]string{
	"movie":   "movie-events",
	"user":    "user-events",
	"payment": "payment-events",
}

var producers map[string]*kafka.Writer

var consumerClosers []func()

type config struct {
	addr         string
	kafkaBrokers []string
}

func main() {
	conf := loadConfig()

	producers = newProducers(conf.kafkaBrokers)
	for _, topic := range eventTopics {
		consumerClosers = append(consumerClosers, startConsumer(conf.kafkaBrokers, topic, "events-service"))
	}

	router := gin.Default()

	eventsRouter := router.Group("/api/events")
	eventsRouter.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": true})
	})
	eventsRouter.POST("/movie", handleMovie)
	eventsRouter.POST("/user", handleUser)
	eventsRouter.POST("/payment", handlePayment)

	srv := &http.Server{
		Addr:              conf.addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Server starting on %s\n", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v\n", err)
	}

	// Закрываем kafka-клиентов после остановки HTTP-сервера.
	for _, closeFn := range consumerClosers {
		closeFn()
	}
	for _, w := range producers {
		_ = w.Close()
	}

	log.Println("Server exited properly")
}

func newProducers(brokers []string) map[string]*kafka.Writer {
	p := map[string]*kafka.Writer{}
	for kind, topic := range eventTopics {
		p[kind] = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			RequiredAcks: kafka.RequireAll,
			Balancer:     &kafka.LeastBytes{},
		}
	}
	return p
}

func publish(ctx context.Context, kind, key string, value []byte) (int, int64, error) {
	w, ok := producers[kind]
	if !ok {
		return 0, 0, fmt.Errorf("unknown event kind: %s", kind)
	}

	msgs := []kafka.Message{{Key: []byte(key), Value: value}}
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		return 0, 0, err
	}
	return msgs[0].Partition, msgs[0].Offset, nil
}

func startConsumer(brokers []string, topic, groupID string) func() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10 << 20,
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				if errors.Is(err, context.Canceled) || err == io.EOF {
					return
				}
				log.Printf("consumer[%s] read error: %v", topic, err)
				continue
			}
			log.Printf("consumed event from %s: key=%s value=%s", topic, string(m.Key), string(m.Value))
		}
	}()

	return func() {
		_ = r.Close()
		<-done
	}
}

func handle(c *gin.Context, kind string) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	partition, offset, err := publish(c.Request.Context(), kind, "", body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	event := gin.H{
		"id":        fmt.Sprintf("%s-%d", kind, offset),
		"type":      kind,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   json.RawMessage(body),
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func handleMovie(c *gin.Context)   { handle(c, "movie") }
func handleUser(c *gin.Context)    { handle(c, "user") }
func handlePayment(c *gin.Context) { handle(c, "payment") }

func loadConfig() *config {
	addr := getEnv("PORT", "8082")
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	return &config{
		addr:         addr,
		kafkaBrokers: brokers,
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
