package framework

import (
	"fmt"
	"os"
	"strconv"
)

var (
	MaxQueue = os.Getenv("MAX_QUEUE")
)

// Job represents the job to be run
type Job struct {
	Payload Payload
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
	Result     chan bool
}

func NewWorker(workerPool chan chan Job, result chan bool) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
		Result:     result}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			//fmt.Printf("registering with worker pool... \n")
			w.WorkerPool <- w.JobChannel
			//fmt.Printf("registered with worker pool... \n")

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				//todo
				//fmt.Sprintf("%s", job)
				error := job.Payload.handlePayload()
				if error != nil {
					w.Result <- false
				} else {
					w.Result <- true
				}

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Payload struct {
	Message *os.FileInfo
}

type InventoryMessage struct {
	StoreNum      string
	Sku           string
	SkuStocklevel int
}

func (payload *Payload) handlePayload() error {
	//fmt.Printf("handle message %i \n", &payload.Message.SkuStocklevel)
	fmt.Printf("handle payload %s", (*payload.Message).Name())
	return nil
	//return payload.Collection.Insert(&payload.Message)
}

//---- worker dispatcher -----
type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	MaxWorkers int
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool, MaxWorkers: maxWorkers}
}

func (d *Dispatcher) Run(workStatus chan bool) {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, workStatus)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			//fmt.Printf("receive job %d \n", job.Payload.Message.SkuStocklevel)
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

//---- worker dispatcher ends-----

func init() {
	maxqueue, error := strconv.Atoi(MaxQueue)
	if error != nil {
		panic("failed to start job queue, couldn't find valid MAX_QUEUE env")
	}

	JobQueue = make(chan Job, maxqueue)
}
