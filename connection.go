package rivers

import (
	"os"
	"time"

	"github.com/beego/redigo/redis"
)

var (
	REDIS_DB  = 0
	Pool      *redis.Pool
	redisDb   = ""
	redisPass = ""
)

func init() {
	redisDb = os.Getenv("RIVERS_REDIS_DB")
	redisPass = os.Getenv("RIVERS_REDIS_PASSWORD")

	Pool = newPool(":6379", redisPass)
}

// Returns a connection pool
func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			// dial redis server
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}

			// authenticate if there's a password
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
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
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
