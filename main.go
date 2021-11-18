//@author nghiatc
//@since Nov 12, 2021

package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"
)

var (
	MaxWorker = 4
	MaxQueue  = 100
)

type Payload struct {
	//Subject string `json:"subject"`
	//To      string `json:"to"`
	//From    string `json:"from"`
	//CC      string `json:"cc"`
	//BCC     string `json:"bcc"`
	//Content string `json:"content"`
}

func (p *Payload) SendEmail() error {
	// Do something
	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
	log.Println("Send email done.")
	return nil
}

// Job represents the job to be run
type Job struct {
	Payload Payload
}

// JobQueue a buffered channel that we can send work requests on.
var JobQueue chan Job

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			log.Println("Register JobChannel into WorkerPool")
			w.WorkerPool <- w.JobChannel
			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				if err := job.Payload.SendEmail(); err != nil {
					log.Printf("Error send email: %s", err.Error())
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

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	maxWorkers int
	WorkerPool chan chan Job
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{maxWorkers: maxWorkers, WorkerPool: pool}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	log.Println("Dispatcher start...")
	for {
		select {
		case job := <-JobQueue:
			log.Printf("A dispatcher request received")
			// A job request has been received
			go func(job Job) {
				// Try to obtain a worker job channel that is available.
				// This will block until a worker is idle
				jobChannel := <-d.WorkerPool
				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

func payloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read the body into a string for json decoding
	var payload Payload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		log.Println("An error occured while deserializing message")
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Println("A valid payload request received")
	// Create a job with the payload
	job := Job{Payload: payload}
	log.Println("Sending job to JobQueue")
	JobQueue <- job
	log.Println("Sent job to JobQueue")
	w.WriteHeader(http.StatusOK)
}

func CreateJob() {
	for {
		// Create a job with the payload
		payload := Payload{}
		job := Job{Payload: payload}
		log.Println("Sending job to JobQueue")
		JobQueue <- job
		log.Println("Sent job to JobQueue")
		time.Sleep(2 * time.Second)
	}
}

func main() {
	// Init JobQueue & Dispatcher
	JobQueue = make(chan Job, MaxQueue)
	dispatcher := NewDispatcher(MaxWorker)
	dispatcher.Run()

	// Generator Job for Test
	go CreateJob()
	go CreateJob()
	go CreateJob()

	// Web Service
	http.HandleFunc("/email", payloadHandler)
	http.ListenAndServe(":8080", nil)
	log.Println("Server is listening on port 8080")

	// Hang thread Main.
	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C) SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)
	// Block until we receive our signal.
	<-c
	log.Println("################# End Main #################")
}
