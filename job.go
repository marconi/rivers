package rivers

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/nu7hatch/gouuid"
)

type Job interface {
	Run()
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
	GetId() string
	GetScore() int64
	Ack() error
	Requeue() error

	// the following are required to encode unexported fields
	// GobEncode() ([]byte, error)
	// GobDecode(buf []byte) error
}

type JobItem struct {
	Id       string
	Schedule time.Time // should be UTC when passed
	Created  time.Time
	queue    Queue
}

// Runs the job
func (j *JobItem) Run() {}

// Serializes the job
func (j *JobItem) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(j); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unserializes the passed data into the job instance
func (j *JobItem) Unmarshal(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(j); err != nil {
		return err
	}
	return nil
}

// Returns the job's id
func (j *JobItem) GetId() string {
	return j.Id
}

// Returns score for delayed queue's job based on schedule
func (j *JobItem) GetScore() int64 {
	return j.Schedule.Unix() - time.Now().UTC().Unix()
}

// Proxies to job's queue for acknowledging
func (j *JobItem) Ack() error {
	return j.queue.(*urgentQueue).Ack(j)
}

// Requeue the job back to urgent queue
func (j *JobItem) Requeue() error {
	return j.queue.(*urgentQueue).Requeue(j)
}

func NewJob(schedule ...time.Time) Job {
	u4, _ := uuid.NewV4()
	var s time.Time
	if len(schedule) > 0 {
		s = schedule[0]
	}
	return &JobItem{
		Id:       u4.String(),
		Schedule: s,
		Created:  time.Now().UTC(),
	}
}
