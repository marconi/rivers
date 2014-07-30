package rivers

import (
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	redisHost = ":6379"
	redisDb   = "0"
	redisPass = ""
	Pool      *redis.Pool
)

func init() {
	redisDb := os.Getenv("RIVERS_REDIS_DB")
	redisPass = os.Getenv("RIVERS_REDIS_PASSWORD")

	if redisDb == "" {
		panic("missing RIVERS_REDIS_DB envar")
	}

	Pool = newPool()
}

func dial() (redis.Conn, error) {
	// dial redis server
	c, err := redis.Dial("tcp", redisHost)
	if err != nil {
		return nil, err
	}

	// authenticate if there's a password
	if redisPass != "" {
		if _, err := c.Do("AUTH", redisPass); err != nil {
			c.Close()
			return nil, err
		}
	}

	// select database
	_, err = c.Do("SELECT", redisDb)
	if err != nil {
		return nil, err
	}

	return c, err
}

// Returns a connection pool
func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial:        dial,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// Return non-pooled connection
func NewNonPool() redis.Conn {
	c, err := dial()
	if err != nil {
		panic(err)
	}
	return c
}
