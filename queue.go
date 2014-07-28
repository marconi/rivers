package rivers

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	PREFIX = "rivers"
)

var (
	TICKTOCK_INTERVAL int64 = 1
)

type Collection interface {
	GetName() string
	GetSize() (int64, error)
	Close() error
}

type Queue interface {
	Collection
	Destroy() error
	Push(j Job) (int64, error)
	Pop() (Job, error)

	// stats related methods
	logStats(name string, n int64)
	startStatsLogger()
	flushStats(now int64)
}

type UrgentQueue interface {
	GetUnackQueue() Collection
	MultiPush(jobs []Job) (int64, error)
}

type DelayedQueue interface {
	GetStoreName() string
	GetIndexName() string
	TickTock() error
	MultiPop(jobIds []string) ([]Job, error)
}

// Queue for popped jobs but haven't been acknowledged
type unackQueue struct {
	name string
	conn redis.Conn
}

// Returns the queue name
func (q *unackQueue) GetName() string {
	return q.name
}

// Returns the number of items in the queue
func (q *unackQueue) GetSize() (int64, error) {
	size, err := redis.Int64(q.conn.Do("HLEN", q.GetName()))
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Closes queue connection
func (q *unackQueue) Close() error {
	if err := q.conn.Close(); err != nil {
		return err
	}
	return nil
}

func (q *unackQueue) remove(j Job) error {
	if _, err := q.conn.Do("HDEL", q.GetName(), j.GetId()); err != nil {
		return err
	}
	return nil
}

// Queue for urgent jobs that needs to be process immediately
type urgentQueue struct {
	unackQueue
	currentQueue   *unackQueue
	statsChan      chan (*statsLog)
	statsCache     map[int64]map[string]int64
	lastStatsFlush int64
}

// Returns the number of items in the queue
func (q *urgentQueue) GetSize() (int64, error) {
	size, err := redis.Int64(q.conn.Do("LLEN", q.GetName()))
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Returns the urgent queue's unack queue
func (q *urgentQueue) GetUnackQueue() Collection {
	return q.currentQueue
}

// Pushes an item to the queue
func (q *urgentQueue) Push(j Job) (int64, error) {
	data, err := j.Marshal()
	if err != nil {
		return 0, err
	}
	n, err := q.conn.Do("LPUSH", q.GetName(), data)
	if err != nil {
		return 0, err
	}

	// increment push stats by one
	q.logStats(PushStatsKey(q.GetName()), 1)

	return n.(int64), nil
}

// Pushes multiple jobs to queue
func (q *urgentQueue) MultiPush(jobs []Job) (int64, error) {
	// encode all jobs
	var marshaledJobs [][]byte
	for _, j := range jobs {
		data, _ := j.Marshal()
		marshaledJobs = append(marshaledJobs, data)
	}

	// push them all at once
	q.conn.Send("MULTI")
	for _, data := range marshaledJobs {
		q.conn.Send("LPUSH", q.GetName(), data)
	}
	r, err := redis.Values(q.conn.Do("EXEC"))
	if err != nil {
		return 0, err
	}

	// increment push stats by one
	q.logStats(PushStatsKey(q.GetName()), int64(len(jobs)))

	return r[len(r)-1].(int64), nil
}

// Pops an item from the queue
func (q *urgentQueue) Pop() (Job, error) {
	// pop job form queue
	item, err := redis.Bytes(q.conn.Do("RPOP", q.GetName()))
	if err != nil {
		return nil, err
	}

	// decode the job
	j := new(JobItem)
	if err = j.Unmarshal(item); err != nil {
		return nil, err
	}

	// whenever a job is popped from an urgent queue,
	// we store reference from that queue on the job
	// so we can ack directly from the job without
	// guessing which queue it came from
	j.queue = q

	// move it to unack queue
	_, err = q.conn.Do("HSET", q.currentQueue.GetName(), j.GetId(), item)
	if err != nil {
		return nil, err
	}

	// increment pop stats by one
	q.logStats(PopStatsKey(q.GetName()), 1)

	// then return popped item
	return j, nil
}

// Acknowledges the job
func (q *urgentQueue) Ack(j Job) error {
	// remove job from unack queue
	if err := q.currentQueue.remove(j); err != nil {
		return err
	}

	// increment ack stats by one
	q.logStats(AckStatsKey(q.GetName()), 1)

	return nil
}

// Requeues the job by proxiying to push
func (q *urgentQueue) Requeue(j Job) error {
	// remove job from unack queue
	if err := q.currentQueue.remove(j); err != nil {
		return err
	}

	// push back the job
	_, err := q.Push(j)

	return err
}

// Closes connection and deletes the queue
func (q *urgentQueue) Destroy() error {
	// get all queue related keys
	keys, err := redis.Strings(q.conn.Do("KEYS", AllQueueKeys(q.GetName())))
	if err != nil {
		return err
	}

	// batch delete all queue related keys and the queue itself
	q.conn.Send("MULTI")
	q.conn.Send("DEL", q.GetName())
	q.conn.Send("DEL", q.currentQueue.GetName())
	for _, key := range keys {
		q.conn.Send("DEL", key)
	}
	if _, err := q.conn.Do("EXEC"); err != nil {
		return err
	}

	// close channels
	close(q.statsChan)

	// close queues
	q.currentQueue.Close()
	q.Close()

	return nil
}

func (q *urgentQueue) logStats(name string, n int64) {
	l := &statsLog{name: name, value: n}
	q.statsChan <- l
}

func (q *urgentQueue) startStatsLogger() {
	// allocate the cache and channel
	q.statsCache = make(map[int64]map[string]int64)
	q.statsChan = make(chan *statsLog, 1024)

	writing := false
	go func() {
		for l := range q.statsChan {
			now := time.Now().UTC().Unix()

			// if now doesn't have a cache yet, allocate one
			if _, ok := q.statsCache[now]; !ok {
				q.statsCache[now] = make(map[string]int64)
			}

			// increment logged stats' value
			q.statsCache[now][l.name] += l.value

			// if we have accumulated some stats and its safe
			// to write, flush the stats cache.
			if now > q.lastStatsFlush && !writing {
				writing = true
				q.flushStats(now)
				writing = false
			}
		}
	}()
}

func (q *urgentQueue) flushStats(now int64) {
	for sec, cache := range q.statsCache {
		// only flush caches older than 2 seconds
		if sec >= now-1 {
			continue
		}

		for name, value := range cache {
			secKey := SecStatsKey(name, sec)
			q.conn.Send("MULTI")
			q.conn.Send("INCRBY", secKey, value)
			q.conn.Send("EXPIRE", secKey, 7200)
			if _, err := q.conn.Do("EXEC"); err != nil {
				log.Println("unable to increment flushed stats for %s:", name, err)
				continue
			}
		}

		// also log queue lengths to expire in two hours
		sizeKey := SecQueueSizeKey(q.GetName(), sec)
		size, err := q.GetSize()
		if _, err = q.conn.Do("SETEX", sizeKey, 7200, size); err != nil {
			log.Println("unable to log queue size:", err)
		}

		// delete this second's cache
		delete(q.statsCache, sec)
	}
	q.lastStatsFlush = now
}

// Queue for jobs with schedule
type delayedQueue struct {
	urgentQueue
	isTicking bool
	quitClock chan (bool)
}

func (q *delayedQueue) GetStoreName() string {
	return fmt.Sprintf("%s:store", q.GetName())
}

func (q *delayedQueue) GetIndexName() string {
	return fmt.Sprintf("%s:index", q.GetName())
}

// Pushes an item to the queue
func (q *delayedQueue) Push(j Job) (int64, error) {
	data, err := j.Marshal()
	if err != nil {
		return 0, err
	}
	score := j.GetScore()
	q.conn.Send("MULTI")
	q.conn.Send("HSET", q.GetStoreName(), j.GetId(), data)
	q.conn.Send("ZADD", q.GetIndexName(), score, j.GetId())
	n, err := redis.Values(q.conn.Do("EXEC"))
	if err != nil {
		return 0, err
	}

	// increment push stats by one
	q.logStats(PushStatsKey(q.GetName()), 1)

	// we're only interested on the result of ZADD which
	// returns the number of items added
	return n[1].(int64), nil
}

// Pops an item from the queue, then its the caller's
// responsibility to push it to an urgent queue.
func (q *delayedQueue) Pop() (Job, error) {
	// check if item from head has zero score
	r, err := redis.Values(q.conn.Do("ZRANGE", q.GetIndexName(), 0, 0, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	// check if there are no items
	if len(r) == 0 {
		return nil, errors.New("queue is empty")
	}

	id, _ := redis.String(r[0], nil)
	score, _ := redis.Int64(r[1], nil)

	// if the score is not zero, then we don't have an item to pop
	if score > 0 {
		return nil, errors.New("no items to be popped")
	}

	q.conn.Send("MULTI")

	// if we have a zero scored item, remove it from the index
	q.conn.Send("ZREM", q.GetIndexName(), id)

	// retrieve data from store
	q.conn.Send("HGET", q.GetStoreName(), id)

	// and then remove from the store
	q.conn.Send("HDEL", q.GetStoreName(), id)

	r, err = redis.Values(q.conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}

	// decode the job
	j := new(JobItem)
	if err = j.Unmarshal(r[1].([]byte)); err != nil {
		return nil, err
	}

	// increment pop stats by one
	q.logStats(PopStatsKey(q.GetName()), 1)

	return j, nil
}

func (q *delayedQueue) MultiPop(jobIds []string) ([]Job, error) {
	q.conn.Send("MULTI")

	for _, id := range jobIds {
		// remove from the index
		q.conn.Send("ZREM", q.GetIndexName(), id)

		// retrieve data from store
		q.conn.Send("HGET", q.GetStoreName(), id)

		// and then remove from the store
		q.conn.Send("HDEL", q.GetStoreName(), id)
	}
	r, err := redis.Values(q.conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}

	var jobs []Job
	for index, _ := range r {
		if (index+1)%3 == 0 {
			j := new(JobItem)
			if err := j.Unmarshal(r[index-1].([]byte)); err != nil {
				log.Println("unable to unmarshal job:", err)
			}
			jobs = append(jobs, j)
		}
	}

	return jobs, nil
}

// Returns the number of items in the queue
func (q *delayedQueue) GetSize() (int64, error) {
	size, err := redis.Int64(q.conn.Do("ZCARD", q.GetIndexName()))
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Closes connection and deletes the queue
func (q *delayedQueue) Destroy() error {
	// get all queue related keys
	keys, err := redis.Strings(q.conn.Do("KEYS", AllQueueKeys(q.GetName())))
	if err != nil {
		return err
	}

	// batch delete all queue related keys and the queue itself
	q.conn.Send("MULTI")
	q.conn.Send("DEL", q.GetStoreName())
	q.conn.Send("DEL", q.GetIndexName())
	for _, key := range keys {
		q.conn.Send("DEL", key)
	}
	if _, err := q.conn.Do("EXEC"); err != nil {
		return err
	}

	close(q.quitClock)
	q.Close()
	return nil
}

// Decrements all jobs' score by one
func (q *delayedQueue) TickTock() error {
	// if ticktock is running, don't run another one
	if q.isTicking {
		return nil
	}

	q.isTicking = true

	// there are multiple returns, but whichever return
	// gets hit, make sure isTicking is switched off
	defer func() {
		q.isTicking = false
	}()

	// get all job ids
	//
	// NOTE: loading all ids into memory might be an overhead
	// if the set is big, ZSCAN might be able to help on this
	idToScore := GetQueueScores(q.GetIndexName(), q.conn)

	// decrement each job's score
	var expiredJobIds []string
	q.conn.Send("MULTI")
	for id, score := range idToScore {
		// only decrement scores > zero so we
		// don't ran into negative scores
		if score > 0 {
			q.conn.Send("ZINCRBY", q.GetIndexName(), -1, id)
		}

		// if score is already zero are about to become zero,
		// collect its job ids so we can bulk pop and push
		// them to urgent queue.
		if score <= 1 {
			expiredJobIds = append(expiredJobIds, id)
		}
	}
	if _, err := q.conn.Do("EXEC"); err != nil {
		return err
	}

	return nil
}

// Starts the ticker which moves jobs from delayed to urgent queue
func (q *delayedQueue) startTicker() {
	ticker := time.NewTicker(time.Duration(TICKTOCK_INTERVAL) * time.Second) // tick every second
	q.quitClock = make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := q.TickTock(); err != nil {
					log.Println("Ticker unable to tock:", err)
				}
			case <-q.quitClock:
				ticker.Stop()
				return
			}
		}
	}()
}

func NewQueue(name, qtype string) Queue {
	var q Queue
	switch qtype {
	case "urgent":
		uqName := fmt.Sprintf("%s:urgent:%s", PREFIX, name)
		q = &urgentQueue{
			unackQueue: unackQueue{
				name: uqName,
				conn: Pool.Get(),
			},
			currentQueue: &unackQueue{
				name: fmt.Sprintf("%s:unack", uqName),
				conn: Pool.Get(),
			},
		}
	case "delayed":
		q = &delayedQueue{
			urgentQueue: urgentQueue{
				unackQueue: unackQueue{
					name: fmt.Sprintf("%s:delayed:%s", PREFIX, name),
					conn: Pool.Get(),
				},
			},
		}
		q.(*delayedQueue).startTicker()
	default:
		panic("invalid queue type")
	}

	// spawn the stats logger for this queue
	q.startStatsLogger()

	return q
}

// Return map with id: score mapping of delayed queue
func GetQueueScores(name string, conn redis.Conn) map[string]int64 {
	var lastJobId string
	scoresMap := make(map[string]int64)
	values, _ := redis.Strings(conn.Do("ZRANGE", name, 0, -1, "WITHSCORES"))
	for index, val := range values {
		if index%2 == 0 {
			scoresMap[val] = 0
			lastJobId = val
		} else {
			i, _ := strconv.ParseInt(val, 10, 64)
			scoresMap[lastJobId] = i
		}
	}
	return scoresMap
}
