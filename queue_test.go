package rivers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/beego/redigo/redis"
	. "github.com/smartystreets/goconvey/convey"

	. "github.com/marconi/rivers"
)

func TestQueue(t *testing.T) {
	// run test on db 1
	var origRedisDb = REDIS_DB
	REDIS_DB = 1

	// then rese tit back after this suite
	defer func() {
		REDIS_DB = origRedisDb
	}()

	Convey("given an urgent queue", t, func() {
		Convey("should have a name", func() {
			uq1 := NewQueue("uq1", "urgent")
			So(uq1.GetName(), ShouldEqual, PREFIX+":urgent:uq1")

			Reset(func() {
				uq1.Destroy()
			})
		})

		Convey("should be able to push item", func() {
			uq2 := NewQueue("uq2", "urgent")
			n, err := uq2.Push(NewJob())
			So(n, ShouldEqual, 1)
			So(err, ShouldEqual, nil)

			Reset(func() {
				uq2.Destroy()
			})
		})

		Convey("should be able to pop item", func() {
			uq3 := NewQueue("uq3", "urgent")
			uq4 := NewQueue("uq4", "urgent")
			uq3.Push(NewJob())

			size, _ := uq3.GetSize()
			So(size, ShouldEqual, 1)

			size, _ = uq4.GetSize()
			So(size, ShouldEqual, 0)

			unackQ := uq3.(UrgentQueue).GetUnackQueue()
			size, _ = unackQ.GetSize()
			So(size, ShouldEqual, 0)

			j, err := uq3.Pop()
			So(j, ShouldNotEqual, nil)
			So(err, ShouldEqual, nil)

			_, err = uq4.Push(j)
			So(err, ShouldEqual, nil)

			size, _ = uq3.GetSize()
			So(size, ShouldEqual, 0)

			size, _ = uq4.GetSize()
			So(size, ShouldEqual, 1)

			size, _ = unackQ.GetSize()
			So(size, ShouldEqual, 1)

			Reset(func() {
				uq3.Destroy()
				uq4.Destroy()
			})
		})

		Convey("should be able to destroy", func() {
			uq5 := NewQueue("uq5", "urgent")
			uq5.Push(NewJob())

			conn := Pool.Get()
			b, _ := redis.Bool(conn.Do("EXISTS", uq5.GetName()))
			So(b, ShouldEqual, true)

			uq5.Destroy()
			b, _ = redis.Bool(conn.Do("EXISTS", uq5.GetName()))
			So(b, ShouldEqual, false)
		})

		Convey("should be able to get size", func() {
			uq6 := NewQueue("uq6", "urgent")
			uq6.Push(NewJob())

			size, _ := uq6.GetSize()
			So(size, ShouldEqual, 1)

			Reset(func() {
				uq6.Destroy()
			})
		})

		Convey("should be able to flush stats", func() {
			conn := Pool.Get()

			Convey("push without delay", func() {
				min := time.Now().UTC().Unix()

				uq7 := NewQueue("uq7", "urgent")
				uq7.Push(NewJob())
				uq7.Push(NewJob())

				now := time.Now().UTC().Unix()
				diff := now - min

				// check the before min up to max,
				// there shouldn't be any stats logged
				for i := int64(0); i <= diff; i++ {
					sec := min + i
					secKey := SecStatsKey(PushStatsKey(uq7.GetName()), sec)
					exists, _ := redis.Bool(conn.Do("EXISTS", secKey))
					So(exists, ShouldEqual, false)

					// there should be no queue size logged as well
					sizeKey := SecQueueSizeKey(uq7.GetName(), sec)
					exists, _ = redis.Bool(conn.Do("EXISTS", sizeKey))
					So(exists, ShouldEqual, false)
				}

				Reset(func() {
					uq7.Destroy()
				})
			})

			Convey("push with 2 seconds delay", func() {
				min := time.Now().UTC().Unix()

				uq8 := NewQueue("uq8", "urgent")
				uq8.Push(NewJob())

				// sleep two seconds to trigger a flush on next push
				time.Sleep(2 * time.Second)

				uq8.Push(NewJob())

				now := time.Now().UTC().Unix()
				diff := now - min

				// sleep one more to make sure flush is done
				// before we actually start checking
				time.Sleep(1 * time.Second)

				// check the before min up to max,
				// there should be stats logged
				foundSecKey := false
				foundSizeKey := false
				for i := int64(0); i <= diff; i++ {
					sec := min + i
					secKey := SecStatsKey(PushStatsKey(uq8.GetName()), sec)
					secExists, _ := redis.Bool(conn.Do("EXISTS", secKey))
					if secExists {
						foundSecKey = true

						pushStats, _ := redis.Int64(conn.Do("GET", secKey))
						So(pushStats, ShouldEqual, 1)
					}

					// there should be queue size logged as well
					sizeKey := SecQueueSizeKey(uq8.GetName(), sec)
					sizeExists, _ := redis.Bool(conn.Do("EXISTS", sizeKey))
					if sizeExists {
						foundSizeKey = true

						// due to the sleep we can be sure that both items
						// has been flushed so we check for size of two here
						sizeStats, _ := redis.Int64(conn.Do("GET", sizeKey))
						So(sizeStats, ShouldEqual, 2)
					}
				}

				So(foundSecKey, ShouldEqual, true)
				So(foundSizeKey, ShouldEqual, true)

				Reset(func() {
					uq8.Destroy()
				})
			})

			Convey("with pop stats", func() {
				min := time.Now().UTC().Unix()

				uqdq1 := NewQueue("uqdq1", "delayed")
				uq9 := NewQueue("uq9", "urgent")
				uqdq1.Push(NewJob())
				uqdq1.Push(NewJob())

				j1, err := uqdq1.Pop()
				So(j1, ShouldNotEqual, nil)
				So(err, ShouldEqual, nil)

				_, err = uq9.Push(j1)
				So(err, ShouldEqual, nil)

				j2, err := uqdq1.Pop()
				So(j2, ShouldNotEqual, nil)
				So(err, ShouldEqual, nil)

				_, err = uq9.Push(j2)
				So(err, ShouldEqual, nil)

				// sleep two seconds to trigger a flush on next push
				time.Sleep(2 * time.Second)

				// that would be 3 push and 2 pop jobs flushed
				uqdq1.Push(NewJob())

				now := time.Now().UTC().Unix()
				diff := now - min

				// sleep one more to make sure flush is done
				// before we actually start checking
				time.Sleep(1 * time.Second)

				// check the before min up to max,
				// there should be stats logged for pop
				foundSecKey := false
				foundSizeKey := false
				for i := int64(0); i <= diff; i++ {
					sec := min + i
					secKey := SecStatsKey(PopStatsKey(uqdq1.GetName()), sec)
					secExists, _ := redis.Bool(conn.Do("EXISTS", secKey))
					if secExists {
						foundSecKey = true

						// we sleep after popping so we can
						// be sure both pop were flushed
						popStats, _ := redis.Int64(conn.Do("GET", secKey))
						So(popStats, ShouldEqual, 2)
					}

					// there should be queue size logged for uqdq1
					sizeKey := SecQueueSizeKey(uqdq1.GetName(), sec)
					sizeExists, _ := redis.Bool(conn.Do("EXISTS", sizeKey))
					if sizeExists {
						foundSizeKey = true

						// we popped twice so uqdq1 should have zero size
						sizeStats, _ := redis.Int64(conn.Do("GET", sizeKey))
						So(sizeStats, ShouldEqual, 0)
					}
				}

				So(foundSecKey, ShouldEqual, true)
				So(foundSizeKey, ShouldEqual, true)

				Reset(func() {
					uqdq1.Destroy()
					uq9.Destroy()
				})
			})

			Reset(func() {
				conn.Close()
			})
		})

		Convey("should be able to ack jobs", func() {
			uq10 := NewQueue("uq10", "urgent")
			uq10.Push(NewJob())
			uq10.Push(NewJob())

			size, _ := uq10.(UrgentQueue).GetUnackQueue().GetSize()
			So(size, ShouldEqual, 0)

			j, _ := uq10.Pop()
			So(j, ShouldNotEqual, nil)

			size, _ = uq10.(UrgentQueue).GetUnackQueue().GetSize()
			So(size, ShouldEqual, 1)

			err := j.Ack()
			So(err, ShouldEqual, nil)

			time.Sleep(1 * time.Second)

			size, _ = uq10.(UrgentQueue).GetUnackQueue().GetSize()
			So(size, ShouldEqual, 0)

			Reset(func() {
				uq10.Destroy()
			})
		})

		Convey("should be able to requeue jobs", func() {
			uq11 := NewQueue("uq11", "urgent")
			uq11.Push(NewJob())
			uq11.Push(NewJob())

			size, _ := uq11.GetSize()
			So(size, ShouldEqual, 2)

			j, _ := uq11.Pop()
			size, _ = uq11.(UrgentQueue).GetUnackQueue().GetSize()
			So(size, ShouldEqual, 1)

			size, _ = uq11.GetSize()
			So(size, ShouldEqual, 1)

			err := j.(*JobItem).Requeue()
			So(err, ShouldEqual, nil)

			size, _ = uq11.GetSize()
			So(size, ShouldEqual, 2)

			size, _ = uq11.(UrgentQueue).GetUnackQueue().GetSize()
			So(size, ShouldEqual, 0)

			Reset(func() {
				uq11.Destroy()
			})
		})
	})

	Convey("given a delayed queue", t, func() {
		Convey("should be able to push item", func() {
			dq1 := NewQueue("dq1", "delayed")
			n, err := dq1.Push(NewJob())

			So(n, ShouldEqual, 1)
			So(err, ShouldEqual, nil)

			Reset(func() {
				dq1.Destroy()
			})
		})

		Convey("should be able to destroy", func() {
			dq2 := NewQueue("dq2", "delayed")
			dq2.Push(NewJob())

			conn := Pool.Get()

			conn.Send("MULTI")
			conn.Send("EXISTS", fmt.Sprintf("%s:store", dq2.GetName()))
			conn.Send("EXISTS", fmt.Sprintf("%s:index", dq2.GetName()))
			r, _ := redis.Values(conn.Do("EXEC"))
			store, index := r[0].(int64), r[1].(int64)
			So(store, ShouldEqual, 1)
			So(index, ShouldEqual, 1)

			dq2.Destroy()

			conn.Send("MULTI")
			conn.Send("EXISTS", fmt.Sprintf("%s:store", dq2.GetName()))
			conn.Send("EXISTS", fmt.Sprintf("%s:index", dq2.GetName()))
			r, _ = redis.Values(conn.Do("EXEC"))
			store, index = r[0].(int64), r[1].(int64)
			So(store, ShouldEqual, 0)
			So(index, ShouldEqual, 0)

			Reset(func() {
				conn.Close()
			})
		})

		Convey("should be able to get size", func() {
			dq3 := NewQueue("dq3", "delayed")
			dq3.Push(NewJob())

			size, _ := dq3.GetSize()
			So(size, ShouldEqual, 1)

			Reset(func() {
				dq3.Destroy()
			})
		})

		Convey("should be able to pop item", func() {
			dq4 := NewQueue("dq4", "delayed")
			dquq1 := NewQueue("dquq1", "urgent")
			dq4.Push(NewJob())

			size, _ := dq4.GetSize()
			So(size, ShouldEqual, 1)

			j, err := dq4.Pop()
			So(j, ShouldNotEqual, nil)
			So(err, ShouldEqual, nil)

			_, err = dquq1.Push(j)
			So(err, ShouldEqual, nil)

			size, _ = dq4.GetSize()
			So(size, ShouldEqual, 0)

			size, _ = dquq1.GetSize()
			So(size, ShouldEqual, 1)

			Reset(func() {
				dq4.Destroy()
				dquq1.Destroy()
			})
		})

		Convey("should be able to ticktock items", func() {
			// increase so we can do our manual
			// ticktock first before the ticker
			TICKTOCK_INTERVAL = 60

			dq5 := NewQueue("dq5", "delayed")
			schedule := time.Now().UTC().Add(2 * time.Second)
			dq5.Push(NewJob(schedule))
			dq5.Push(NewJob(schedule))

			conn := Pool.Get()
			scoresMap := GetQueueScores(dq5.(DelayedQueue).GetIndexName(), conn)

			err := dq5.(DelayedQueue).TickTock()
			So(err, ShouldEqual, nil)

			newScoresMap := GetQueueScores(dq5.(DelayedQueue).GetIndexName(), conn)
			for id, score := range newScoresMap {
				So(score, ShouldBeLessThan, scoresMap[id])
				So(score, ShouldEqual, scoresMap[id]-1)
			}

			Reset(func() {
				TICKTOCK_INTERVAL = 1 // revert

				conn.Close()
				dq5.Destroy()
			})
		})

		Convey("ticker should be ticking", func() {
			dq6 := NewQueue("dq6", "delayed")
			dq6.Push(NewJob(time.Now().UTC().Add(1 * time.Minute)))
			dq6.Push(NewJob(time.Now().UTC().Add(2 * time.Minute)))

			conn := Pool.Get()
			scoresMap := GetQueueScores(dq6.(DelayedQueue).GetIndexName(), conn)

			time.Sleep(2 * time.Second)

			newScoresMap := GetQueueScores(dq6.(DelayedQueue).GetIndexName(), conn)
			for id, score := range newScoresMap {
				// other operations are in the way, we can't accurate
				// measure the offset so we just settle for less than.
				So(score, ShouldBeLessThan, scoresMap[id])
			}

			Reset(func() {
				conn.Close()
				dq6.Destroy()
			})
		})

		Convey("should be able to multi-pop", func() {
			j1 := NewJob(time.Now().UTC().Add(1 * time.Second))
			j2 := NewJob(time.Now().UTC().Add(2 * time.Second))

			dquq2 := NewQueue("dquq2", "urgent")
			dq7 := NewQueue("dq7", "delayed")
			dq7.Push(j1)
			dq7.Push(j2)

			size, _ := dq7.GetSize()
			So(size, ShouldEqual, 2)

			jobs, err := dq7.(DelayedQueue).MultiPop([]string{j1.GetId(), j2.GetId()})

			So(jobs, ShouldNotEqual, nil)
			So(err, ShouldEqual, nil)
			So(len(jobs), ShouldEqual, 2)

			size, _ = dq7.GetSize()
			So(size, ShouldEqual, 0)

			Convey("should be able to multi-push", func() {
				size, _ := dquq2.GetSize()
				So(size, ShouldEqual, 0)

				n, err := dquq2.(UrgentQueue).MultiPush(jobs)
				So(err, ShouldEqual, nil)
				So(n, ShouldEqual, 2)

				size, _ = dquq2.GetSize()
				So(size, ShouldEqual, 2)
			})

			Reset(func() {
				dq7.Destroy()
				dquq2.Destroy()
			})
		})
	})
}
