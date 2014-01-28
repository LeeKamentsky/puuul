"""PUUUL - a thread pool

Puuul is a library for managing the lifetime of a thread pool and for placing
tasks on that pool. The library provides support for compositing tasks,
running the tasks on the thread pool, receiving notification when complete
and synchronizing and joining.
"""
# (c) Copyright 2014 Broad Institute all rights reserved.
#
# Copyright under terms of the MIT license - see LICENSE for details
#
from ._task import Task, is_inside_task_run
import threading
import collections
import functools

class ThreadPool(object):
    def __init__(self, nthreads=4):
        self.__ready_task_q = collections.deque()
        self.__condition = threading.Condition()
        self.__monitor_condition = threading.Condition()
        self.__started = False
        self.__shutting_down = False
        self.__monitor_thread_q = collections.deque()
        self.__nthreads = nthreads
        self.__running_tasks = []

    def start(self):
        '''Start the thread pool's threads'''
        assert not self.__started
        self.__monitor_thread = threading.Thread(
            name = "PUUUL monitor thread",
            target=self.monitor_thread_loop)
        self.__worker_threads = [
            threading.Thread(
                name = "PUUUL worker thread #%d" % i,
                target=self.worker_thread_loop)
            for i in range(1, self.__nthreads+1)]
        self.__monitor_thread.setDaemon(True)
        self.__monitor_thread.start()
        for t in self.__worker_threads:
            t.setDaemon(True)
            t.start()
        self.__started = True
        
    def stop(self, join=True):
        '''Stop all threads 
        
        join - if True wait for all threads to finish
        '''
        with self.__monitor_condition:
            if self.__started and not self.__shutting_down:
                self.__shutting_down = True
                self.__monitor_condition.notify_all()
        if join:
            self.__monitor_thread.join()
            for thread in self.__worker_threads:
                thread.join()
                
    def __enter__(self):
        self.start()
        return self
        
    def __exit__(self, arg1, arg2, arg3):
        self.stop()
    
    @property
    def n_threads(self):
        '''Number of worker threads in the pool'''
        return self.__nthreads
        
    def do(self, fn, *args, **kwargs):
        '''Run the given function on this thread pool
        
        fn - a function to be run with args and keywoard args supplied
             as additional arguments to do().
             
        If you pass tasks instead of arguments, the ThreadPool will wait
        for those tasks to finish and use the sub-task results as argument
        instead.
             
        returns a Task which will yield its result when you call it. Example:
        
        > print thread_pool.do(sum, [1, 1])()
        
        2
        
        > print thread_pool.do(cmp, thread_pool.do(sum, [1, 2]), thread_pool.do(sum, [3, 4]))

        -1
        
        '''
        task = Task(fn, *args, **kwargs)
        self.submit(task)
        return task
        
    def submit(self, task):
        '''Submit a task to be processed
        
        '''
        if is_inside_task_run() and task.get_state() == Task.STATE_READY:
            # Recursively submitting
            #    There's a danger of thread starvation here. For instance,
            #    say there's a function that uses tasks and that function
            #    is called within a task. If you only have one worker thread,
            #    you will enqueue the subtask from within the worker thread
            #    and the worker thread can never execute the subtask to unblock.
            #
            #    The expedient solution is to run the subtask within the
            #    worker thread. A better solution would be to use greenlets
            #    to switch out of the stack context between the parent Task.run
            #    and the subtask Task.__call__. We could make Task a subclass
            #    of greenlet and enqueue greenlets (and re-enqueue these
            #    blocked greenlets). Effectively, swapping stacks = switching
            #    tasks, so we would use greenlets to make lots of lightweight
            #    tasklets.
            #
            task.run()
            return
        with self.__monitor_condition:
            self.__monitor_thread_q.append(task)
            self.__monitor_condition.notify()
    
    def monitor_thread_loop(self):
        while True:
            tasks = []
            with self.__monitor_condition:
                if self.__shutting_down:
                    break
                if len(self.__monitor_thread_q) == 0:
                    self.__monitor_condition.wait()
                while len(self.__monitor_thread_q) > 0:
                    task = self.__monitor_thread_q.popleft()
                    tasks.append(task)
            for task in tasks:
                assert isinstance(task, Task)
                if task.get_state() == Task.STATE_BLOCKED:
                    task.add_ready_callback(
                        functools.partial(self.submit, task))
                elif task.get_state() == Task.STATE_READY:
                    with self.__condition:
                        task.add_callback(
                            functools.partial(self.submit, task))
                        self.__ready_task_q.append(task)
                        self.__running_tasks.append(task)
                        self.__condition.notify()
                elif task.get_state() != Task.STATE_RUNNING:
                    if task in self.__running_tasks:
                        self.__running_tasks.remove(task)
        #
        # shutdown sequence
        #
        for task in self.__running_tasks:
            assert isinstance(task, Task)
            task.cancel()
        with self.__condition:
            self.__condition.notify_all()
            
    def worker_thread_loop(self):
        while True:
            task = None
            with self.__condition:
                if self.__shutting_down:
                    break
                if len(self.__ready_task_q) == 0:
                    self.__condition.wait()
                if len(self.__ready_task_q) > 0:
                    task = self.__ready_task_q.popleft()
                #
                # if 2 or more, let someone else have a crack at q
                #
                if len(self.__ready_task_q) > 0:
                    self.__condition.notify()
            if task is not None:
                assert isinstance(task, Task)
                task.run()

__default_thread_pool = None
def start(nthreads=4):
    '''Start the default thread pool
    
    nthreads - # of worker threads to use. Defaults to 4.
    '''
    global __default_thread_pool
    
    __default_thread_pool = ThreadPool(nthreads)
    __default_thread_pool.start()
    
def stop(join = True):
    '''Stop the default thread pool
    
    join - wait for threads to terminate before returning. Defaults to True
    '''
    global __default_thread_pool
    if isinstance(__default_thread_pool, ThreadPool):
        __default_thread_pool.stop(join)
        
def do(fn, *args, **kwargs):
    '''Run the given function on this thread pool
    
    fn - a function to be run with args and keywoard args supplied
         as additional arguments to do().
         
    If you pass tasks instead of arguments, the ThreadPool will wait
    for those tasks to finish and use the sub-task results as argument
    instead.
         
    returns a Task which will yield its result when you call it. Example:
    
    > print thread_pool.do(sum, [1, 1])()
    
    2
    
    > print thread_pool.do(cmp, thread_pool.do(sum, [1, 2]), thread_pool.do(sum, [3, 4]))

    -1
    
    '''
    global __default_thread_pool
    assert isinstance(__default_thread_pool, ThreadPool), "Thread pool not started"
    return __default_thread_pool.do(fn, *args, **kwargs)
