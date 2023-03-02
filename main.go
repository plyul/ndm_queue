package main

import (
	"container/list"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const listenPortDefault = 8080

func NewQueue() *Queue {
	q := Queue{}
	q.messageList = list.New()
	return &q
}

type Queue struct {
	mu          sync.Mutex
	messageList *list.List
}

var ErrEmptyQueue = errors.New("queue is empty")

func (q *Queue) Pop() (any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	e := q.messageList.Front()
	if e == nil {
		return "", ErrEmptyQueue
	}
	return q.messageList.Remove(e), nil
}

func (q *Queue) Push(value any) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.messageList.PushBack(value)
}

type queueApp struct {
	listenPort    int
	qmux          sync.Mutex
	messageQueues map[string]*Queue
	waitList      *list.List
}

func NewApp() *queueApp {
	return &queueApp{
		listenPort:    listenPortDefault,
		messageQueues: make(map[string]*Queue),
		waitList:      list.New(),
	}
}

func (app *queueApp) httpHandler(w http.ResponseWriter, r *http.Request) {
	queueName := r.URL.Path // Do not refine queue name, any name matters, so for example '/pet' is a good one too
	switch r.Method {
	case http.MethodGet:
		timeoutString := r.URL.Query().Get("timeout")
		queue := app.queueByName(queueName)
		timeout, _ := strconv.Atoi(timeoutString) // On error, timeout will be equal to zero. Zero is good.
		s, err := app.waitMessage(queue, timeout)
		if err == ErrEmptyQueue {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		io.WriteString(w, s+"\n")

	case http.MethodPut:
		value := r.URL.Query().Get("v")
		if value == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		app.queueByName(queueName).Push(value)
		if app.waitList.Len() > 0 {
			close(app.waitList.Front().Value.(chan struct{}))
		}
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (app *queueApp) queueByName(queueName string) *Queue {
	app.qmux.Lock()
	defer app.qmux.Unlock()
	queue := app.messageQueues[queueName]
	if queue == nil {
		app.messageQueues[queueName] = NewQueue()
		queue = app.messageQueues[queueName]
	}

	return queue
}

func (app *queueApp) waitMessage(queue *Queue, timeout int) (string, error) {
	v, err := queue.Pop()
	if err == nil {
		return v.(string), err
	}
	if err == ErrEmptyQueue && timeout == 0 {
		return v.(string), err
	}
	notifyCh := make(chan struct{})
	timer := time.NewTimer(time.Duration(timeout) * time.Second)
	ticket := app.waitList.PushBack(notifyCh)
	select {
	case <-timer.C:
		app.waitList.Remove(ticket)
		return "", ErrEmptyQueue
	case <-notifyCh:
		app.waitList.Remove(ticket)
		v, err := queue.Pop()
		return v.(string), err
	}
}

func (app *queueApp) Run() error {
	http.HandleFunc("/", app.httpHandler) // Use global DefaultServeMux for simplicity
	return http.ListenAndServe(fmt.Sprintf(":%d", app.listenPort), nil)
}

func (app *queueApp) WithPort(port int) *queueApp {
	app.listenPort = port
	return app
}

func main() {
	port := flag.Int("port", listenPortDefault, "Port to listen incoming requests")
	flag.Parse()

	err := NewApp().WithPort(*port).Run()
	if err != http.ErrServerClosed {
		fmt.Printf("Error running application: %s\n", err)
	}
}
