package main

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	addr        string
	monolithUrl string
	moviesUrl   string
	eventsUrl   string

	gradualMigration   bool
	moviesMigrationPct int
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	conf := loadConfig()

	router := gin.Default()
	router.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "Proxy is healthy")
	})

	router.Any("/api/movies", moviesHandler(conf))

	router.Any("/api/movies/health", func(c *gin.Context) { proxyTo(c, conf.moviesUrl) })

	// Не вынесены из монолита, остаются на нём.
	router.Any("/api/users", func(c *gin.Context) { proxyTo(c, conf.monolithUrl) })
	router.Any("/api/payments", func(c *gin.Context) { proxyTo(c, conf.monolithUrl) })
	router.Any("/api/subscriptions", func(c *gin.Context) { proxyTo(c, conf.monolithUrl) })

	srv := &http.Server{
		Addr:              conf.addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("proxy listening on %s", conf.addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("proxy stopped")
}

func moviesHandler(conf *config) gin.HandlerFunc {
	return func(c *gin.Context) {
		target := conf.monolithUrl
		servedBy := "monolith"

		if shouldRouteToMovies(conf) {
			target = conf.moviesUrl
			servedBy = "movies-service"
		}

		log.Printf("[strangler] %s %s -> %s", c.Request.Method, c.Request.URL.RequestURI(), servedBy)
		c.Writer.Header().Set("X-Served-By", servedBy)
		proxyTo(c, target)
	}
}

func shouldRouteToMovies(conf *config) bool {
	if !conf.gradualMigration {
		return false
	}
	if conf.moviesMigrationPct >= 100 {
		return true
	}
	if conf.moviesMigrationPct <= 0 {
		return false
	}
	return rng.Intn(100) < conf.moviesMigrationPct
}

func proxyTo(c *gin.Context, target string) {
	targetURL, err := url.Parse(target)
	if err != nil {
		log.Printf("invalid backend URL %q: %v", target, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "Bad gateway"})
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Host = targetURL.Host
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error to %s: %v", target, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"Bad gateway"}`))
	}

	proxy.ServeHTTP(c.Writer, c.Request)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	i, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return i
}

func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}
	return b
}

func loadConfig() *config {
	addr := getEnv("PORT", "8000")
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}

	return &config{
		addr:               addr,
		monolithUrl:        getEnv("MONOLITH_URL", "http://localhost:8080"),
		moviesUrl:          getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"),
		eventsUrl:          getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"),
		gradualMigration:   getEnvBool("GRADUAL_MIGRATION", false),
		moviesMigrationPct: getEnvInt("MOVIES_MIGRATION_PERCENT", 0),
	}
}
