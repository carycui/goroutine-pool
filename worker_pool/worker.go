package worker_pool

import (
	_"log"
)

type Task func() error

// Worker is the actual executor who runs the task,
// it starts a goroutine that accepts tasks and
// performs task's function calls.
type Worker struct {
	// The worker pool that owns the worker
	pool *WorkerPool
	// The task should be done
	taskChan chan Task
}

// Create a new worker with specified worker pool
func newWorker(p *WorkerPool) (*Worker, error) {
	w := &Worker{
		pool : p,
		taskChan : make(chan Task,1),
	}
	// Tell the pool that the worker is started
	p.onWorkerStarted(w)
	// Start worker's looper in goroutine
	go w.runLooper()
	return w,nil
}

// Worker's run looper 
// it accepts task and run it
// exit looper when the task is nil
func (w* Worker) runLooper() {
	for {
		select {
		case t := <-w.taskChan :
			if t == nil {
				// Tell pool that worker is closed
				w.pool.onWorkerClosed(w)
				return
			}
			// Execute the task
			t()
			// Tell pool that worker is released, ready for another task
			w.pool.onWorkerReleased(w)
		}
	}
}

// Run the task
func (w *Worker) runTask(t Task) {
	w.taskChan <- t
}
