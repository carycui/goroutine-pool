package worker_pool

import (
	"errors"
	"sync"
	_"log"
)

// WorkerPool accept the tasks from client,it limits the total
// of goroutines to a given number.
type WorkerPool struct {
	// pool capacity
	capacity 		int32
	// push task channel 
	taskChan		chan Task
	// array that hold all the workers 
	workers     	[]*Worker
	// idle worker queue
	idleWorkerQueue chan *Worker
	// workers wait group
	wgWorker		sync.WaitGroup
	// tasks wait group
	wgTask			sync.WaitGroup
}

// Create a worker pool with limited number
//
func NewWorkerPool(size int32) (*WorkerPool, error) {
	if size <=0 {
		return nil, errors.New("Invalid size for worker pool")
	}

	p := &WorkerPool {
		capacity : size,
		taskChan : make (chan Task, size),
		idleWorkerQueue : make(chan *Worker, size),
	}	
	// Create workers and push to the idle queue
	var i int32
	for i = 0; i < size; i++ {
		w,e := newWorker(p)
		if e == nil {
			p.workers = append(p.workers,w)
			p.idleWorkerQueue <- w
		}
	}
	// Start run looper
	go p.runLooper()
	return p, nil
}

// Run one task in the worker pool
//
func (p* WorkerPool) RunTask(t Task) error {
	if t==nil {
		return errors.New("task is invalid")
	}
	p.taskChan <- t
	p.wgTask.Add(1)
	return nil
}

// Wait until all tasks are done
// 
func (p* WorkerPool) WaitTasksDone() {
	// Wait all taskes are done
	p.wgTask.Wait()
}

// Close the worker pool
// it will wait all tasks and workers to be done
func (p* WorkerPool) Close() {
	// Wait all taskes are done
	p.wgTask.Wait()
	// Send nil task to close 
	p.taskChan <- nil
	// Wait all workers are done
	p.wgWorker.Wait()
}

// Worker pool run looper
func (p *WorkerPool) runLooper () {
	for {
		select {
		case t := <-p.taskChan :
			if t==nil {
				p.close()
				return
			}
			p.assignWorkerForTask(t)
		}
	}
}
// Interal close, notify all workers to exit run looper
func (p *WorkerPool) close() {
	for _,w := range p.workers {
		w.runTask(nil)
	}
}

// Assign task to one idle worker to execute it
func (p *WorkerPool) assignWorkerForTask(t Task) {
	select {
	case w := <-p.idleWorkerQueue:
		w.runTask(t)
	}
}

// Callback for worker is released
func (p *WorkerPool) onWorkerReleased(w* Worker) {
	// Mark one task is done
	p.wgTask.Done()
	// Push back the worker into the idle queue
	p.idleWorkerQueue <- w
}

// Callback for worker is started
func (p *WorkerPool) onWorkerStarted(w* Worker) {
	// Mark one task is done
	p.wgWorker.Add(1)
}

// Callback for worker is closed
func (p *WorkerPool) onWorkerClosed(w* Worker) {
	p.wgWorker.Done()
}