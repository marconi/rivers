package rivers

import (
	"errors"
	"fmt"
	"log"
	"reflect"
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
	Register(name string, handlerFunc interface{})
}

type UrgentQueue interface {
	GetUnackQueue() Collection
	MultiPush(jobs []Job, conn ...redis.Conn) (int64, error)
}

type DelayedQueue interface {
	GetStoreName() string
	GetIndexName() string
	TickTock(conn redis.Conn) error
	MultiPop(conn ...redis.Conn) ([]Job, error)
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
	currentQueue *unackQueue

	// hooks mapping
	hooks map[string][]interface{}
}

func (q *urgentQueue) Register(name string, handlerFunc interface{}) {
	q.hooks[name] = append(q.hooks[name], handlerFunc)
}

// Executes each registered handler for the hook
func (q *urgentQueue) runHooks(name string, params ...interface{}) {
	if handlers, ok := q.hooks[name]; ok {
		// running of hooks shouldn't block the flow
		go func() {
			// convert each param to reflect.Value
			var valParams []reflect.Value
			for _, param := range params {
				valParams = append(valParams, reflect.ValueOf(param))
			}

			// invoke handler
			for _, handler := range handlers {
				reflect.ValueOf(handler).Call(valParams)
			}
		}()
	}
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

	q.runHooks("push", j)

	return n.(int64), nil
}

// Pushes multiple jobs to queue
func (q *urgentQueue) MultiPush(jobs []Job, conn ...redis.Conn) (int64, error) {
	// default connection to the polled one from the queue
	qconn := q.conn

	// if a connection has been passed, override default connection
	// this is because using MultiPush with default connection
	// inside goroutine wouldn't work due to polled-connection
	// not supporting concurrency. This way client can pass non-pooled
	// connection for override.
	if len(conn) > 0 {
		qconn = conn[0]
	}

	// encode all jobs
	var marshaledJobs [][]byte
	for _, j := range jobs {
		data, _ := j.Marshal()
		marshaledJobs = append(marshaledJobs, data)
	}

	// push them all at once
	qconn.Send("MULTI")
	for _, data := range marshaledJobs {
		qconn.Send("LPUSH", q.GetName(), data)
	}
	r, err := redis.Values(qconn.Do("EXEC"))
	if err != nil {
		return 0, err
	}

	q.runHooks("multipush", jobs)

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

	q.runHooks("pop", j)

	// then return popped item
	return j, nil
}

// Acknowledges the job
func (q *urgentQueue) Ack(j Job) error {
	// remove job from unack queue
	if err := q.currentQueue.remove(j); err != nil {
		return err
	}

	q.runHooks("ack", j)

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

	// close queues
	q.currentQueue.Close()
	q.Close()

	return nil
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

	// invoke registered handlers
	q.runHooks("push", j)

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

	// invoke registered handlers
	q.runHooks("pop", j)

	return j, nil
}

// Pops multiple zero-scored jobs from the queue
func (q *delayedQueue) MultiPop(conn ...redis.Conn) ([]Job, error) {
	// default connection to the polled one from the queue
	qconn := q.conn

	// if a connection has been passed, override default connection
	// this is because using MultiPop with default connection
	// inside goroutine wouldn't work due to polled-connection
	// not supporting concurrency. This way client can pass non-pooled
	// connection for override.
	if len(conn) > 0 {
		qconn = conn[0]
	}

	// get all zero scored jobs
	jobIds, err := redis.Strings(qconn.Do("ZRANGEBYSCORE", q.GetIndexName(), 0, 0))
	if err != nil {
		return nil, err
	}

	// NOTE: Single transaction should be good here
	// but for some reason the redis client can't handle
	// multi-multi return values from single transaction.
	// Something to look into.
	var jobs []Job
	for _, id := range jobIds {
		qconn.Send("MULTI")
		// remove from the index
		qconn.Send("ZREM", q.GetIndexName(), id)

		// retrieve data from store
		qconn.Send("HGET", q.GetStoreName(), id)

		// and then remove from the store
		qconn.Send("HDEL", q.GetStoreName(), id)

		r, err := redis.Values(qconn.Do("EXEC"))
		if err != nil {
			log.Println("error popping job", id+":", err)
		} else {
			j := new(JobItem)
			if err := j.Unmarshal(r[1].([]byte)); err != nil {
				log.Println("unable to unmarshal job", id+":", err)
			}
			jobs = append(jobs, j)
		}
	}

	// invoke registered handlers
	q.runHooks("multipop", jobs)

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
func (q *delayedQueue) TickTock(conn redis.Conn) error {
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
	idToScore := GetQueueScores(q.GetIndexName(), conn)

	// decrement each job's score
	conn.Send("MULTI")
	for id, score := range idToScore {
		// only decrement scores > zero so we
		// don't ran into negative scores
		if score > 0 {
			conn.Send("ZINCRBY", q.GetIndexName(), -1, id)
		}
	}
	if _, err := conn.Do("EXEC"); err != nil {
		return err
	}

	return nil
}

// Starts the ticker which moves jobs from delayed to urgent queue
func (q *delayedQueue) startTicker() {
	// redigo doesn't support concurrency on polled connection
	// see https://github.com/garyburd/redigo/issues/73
	tickerConn := NewNonPool()

	// tick every second
	ticker := time.NewTicker(time.Duration(TICKTOCK_INTERVAL) * time.Second)
	q.quitClock = make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := q.TickTock(tickerConn); err != nil {
					log.Println("Ticker unable to tock:", err)
				}
			case <-q.quitClock:
				ticker.Stop()
				tickerConn.Close()
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
			hooks: make(map[string][]interface{}),
		}
	case "delayed":
		q = &delayedQueue{
			urgentQueue: urgentQueue{
				unackQueue: unackQueue{
					name: fmt.Sprintf("%s:delayed:%s", PREFIX, name),
					conn: Pool.Get(),
				},
				hooks: make(map[string][]interface{}),
			},
		}
		q.(*delayedQueue).startTicker()
	default:
		panic("invalid queue type")
	}

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
