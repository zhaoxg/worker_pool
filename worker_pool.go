package util

/**
 * 提供一个比较通用的 goroutine pool
 * 暂时不考虑错误处理
 */

import (
//    "fmt"
    "sync"
)

type JobRunner interface {
    Run()
}

type JobResult interface {
    GetResult()
}


type WorkerPool struct {
    // Job 队列
    JobQueue chan JobRunner
    //worker队列
    WorkerQueue chan chan JobRunner
    //结果队列
    ResponseQueue chan JobResult
    //一个全局的退出标识
    QuitChan chan bool

    wg sync.WaitGroup

    jobCount int
    workerCount int
}

func ( wp *WorkerPool) PostTask(runner JobRunner) {
    wp.JobQueue <- runner
}

func (p *WorkerPool) JobDone() {
    p.wg.Done()
}


func (p *WorkerPool) WaitCount(count int) {
    p.wg.Add(count)
}

func (p *WorkerPool) WaitAll() {
    p.wg.Wait()
}


func (p *WorkerPool) Release() {
    p.QuitChan <- true
}


func NewWorkerPool(jobCount int, workerCount int) *WorkerPool {

    aWorkerPool := WorkerPool{
        JobQueue: make(chan JobRunner, jobCount),
        WorkerQueue: make(chan chan JobRunner, workerCount),
        ResponseQueue: make(chan JobResult, workerCount),
        QuitChan: make(chan bool),
        jobCount: jobCount,
        workerCount:workerCount}

    // Now, create all of our workers.
    for i := 0; i < workerCount; i++ {
        //fmt.Println("Starting worker", i)
        worker := newWorker(i, &aWorkerPool)
        worker.Start()
    }

    go func() {
        for {
            select {
            case job := <- aWorkerPool.JobQueue:
                //fmt.Println("Received work requeust")
                go func() {
                    worker := <- aWorkerPool.WorkerQueue
                    //fmt.Println("Dispatching work request")
                    worker <- job
                }()
            case <- aWorkerPool.QuitChan:
                aWorkerPool.QuitChan<- true
                //fmt.Println("Dispatching got quit signal")
                return
            }
        }
    }()
    return &aWorkerPool
}



func newWorker(id int, pool *WorkerPool) Worker {
    worker := Worker {
        ID: id,
        JobReceiver: make(chan JobRunner),
        WorkerPool:pool}

    return worker
}


type Worker struct {
    ID int
    JobReceiver chan JobRunner
    WorkerPool *WorkerPool
}


func (w *Worker) Start() {
    go func() {
        for {
            //每次循环把自己加入到队列去
            w.WorkerPool.WorkerQueue <- w.JobReceiver
            select {
            case runner := <-w.JobReceiver:
                //fmt.Printf("worker%d: Received work request\n", w.ID)
                runner.Run()
                //fmt.Printf("worker%d: Done\n", w.ID)

            case <-w.WorkerPool.QuitChan:
                //fmt.Printf("worker%d got quit signal\n", w.ID)
                //自己退出同时也让其他退出
                w.WorkerPool.QuitChan<- true
                return
            }
        }
    }()
}