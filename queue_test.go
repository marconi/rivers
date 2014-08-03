package rivers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	. "github.com/smartystreets/goconvey/convey"

	. "github.com/marconi/rivers"
)

func TestQueue(t *testing.T) {
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

			err := dq5.(DelayedQueue).TickTock(NewNonPool())
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
			dquq2 := NewQueue("dquq2", "urgent")
			dq7 := NewQueue("dq7", "delayed")
			dq7.Push(NewJob(time.Now().UTC()))
			dq7.Push(NewJob(time.Now().UTC()))

			time.Sleep(1 * time.Second)

			size, err := dq7.GetSize()
			So(err, ShouldEqual, nil)
			So(size, ShouldEqual, 2)

			jobs, err := dq7.(DelayedQueue).MultiPop()

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

		Convey("should be able to register with hooks", func() {
			dq8 := NewQueue("dq8", "delayed")

			pushCalled, popCalled := false, false
			dq8.Register("push", func(j Job) {
				pushCalled = true
			})
			dq8.Register("pop", func(j Job) {
				popCalled = true
			})

			dq8.Push(NewJob(time.Now().UTC()))
			dq8.Push(NewJob(time.Now().UTC()))
			dq8.Pop()

			time.Sleep(1 * time.Second)

			So(pushCalled, ShouldEqual, true)
			So(popCalled, ShouldEqual, true)

			Reset(func() {
				dq8.Destroy()
			})
		})
	})
}
