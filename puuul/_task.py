"""Task - a task to be performed on a thread
"""
# (c) Copyright 2014 Broad Institute all rights reserved.
#
# Copyright under terms of the MIT license - see LICENSE for details
#
import threading
import functools
from ._cancellation_exception import CancellationException

_thread_local = threading.local()
_lock = threading.RLock()
_running_tasks = []

_K_CURRENT_TASK="CurrentTask"

def check_cancel():
    '''Check to see if the current thread's task has been cancelled
    
    Task functions should periodically call check_cancel to check for
    task cancellation. check_cancel raises a CancellationException
    (which should not be caught) if the task has been cancelled.
    '''
    global _lock, _thread_local
    with _lock:
        task = getattr(_thread_local, _K_CURRENT_TASK)
        if task is not None:
            state = task.get_state()
            assert state in (Task.STATE_CANCELING, Task.STATE_RUNNING), "Task is not in a cancelling or running state. State = " + state
            if task.get_state() == Task.STATE_CANCELING:
                raise CancellationException()
            
def is_inside_task_run():
    '''Return True if the current thread is running a task
    
    Checks to see if this stack frame has been called from a task's run method.
    '''
    global _thread_local
    return hasattr(_thread_local, _K_CURRENT_TASK) and \
           isinstance(getattr(_thread_local, _K_CURRENT_TASK), Task)
        
class Task(object):
    STATE_BLOCKED = "Blocked"
    STATE_READY = "Ready"
    STATE_RUNNING = "Running"
    STATE_CANCELING = "Canceling"
    STATE_CANCELLED = "Cancelled"
    STATE_DONE = "Done"
    STATE_EXCEPTION_RAISED = "Exception raised"
    def __init__(self, fn, *args, **kwargs):
        '''Initializer
        
        Initialize the task with the function to be run and the arguments
        and keyword arguments to be passed in.
        '''
        global _lock
        self.__fn = fn
        self.__args = args
        self.__kwargs = kwargs.copy()
        self.__event = threading.Event()
        self.__block_count = 0
        self.__state = self.STATE_BLOCKED
        self.__callbacks = []
        self.__dependency_callbacks = []
        self.__ready_callbacks = []
        for dependency in (list(args) + list(kwargs.values())):
            if not isinstance(dependency, Task):
                continue
            cb = functools.partial(self.__on_dependency, dependency)
            self.__dependency_callbacks.append((dependency, cb))
            dependency.add_callback(cb)
        with _lock:
            if self.__state == self.STATE_BLOCKED:
                if len(self.__dependency_callbacks) == 0:
                    self.__state = self.STATE_READY
            
    def get_state(self):
        '''The state of the task, e.g. STATE_DONE'''
        return self.__state
    
    def get_exception(self):
        '''Get the exception that was thrown during the run'''
        return self.__exception
    
    def __call__(self):
        '''Return the value of the task'''
        self.__event.wait()
        if self.__state == self.STATE_CANCELLED:
            raise CancellationException()
        if self.__state == self.STATE_EXCEPTION_RAISED:
            raise self.__exception
        return self.__result
    
    def add_callback(self, callback):
        '''Add a callback that will be called on exiting the running state
        
        callback - a closure to be called when we enter the STATE_DONE state,
                   when the task has been cancelled or when it throws
                   an exception.
                   
        The callback is called immediately if the state is one of the final ones
        '''
        global _lock
        with _lock:
            if self.__state in (self.STATE_BLOCKED, self.STATE_READY,
                                self.STATE_RUNNING):
                self.__callbacks.append(callback)
                return
        callback()
        
    def add_ready_callback(self, callback):
        '''Add a function to call when the state changes from blocked to something'''
        global _lock
        with _lock:
            if self.__state == self.STATE_BLOCKED:
                self.__ready_callbacks.append(callback)
                return
        callback()
        
    def remove_callback(self, callback):
        '''Remove a previously added callback
        
        callback - a callback added with add_callback
        '''
        global _lock
        with _lock:
            self.__callbacks.remove(callback)
    
    def cancel(self):
        '''Cancel the task'''
        global _lock
        with _lock:
            if self.__state == self.STATE_RUNNING:
                self.__state = self.STATE_CANCELING
                return
            elif self.__state in (self.STATE_BLOCKED, self.STATE_READY):
                ready_callbacks = self.__ready_callbacks
                self.__ready_callbacks = []
                self.__finish(self.STATE_CANCELLED)
        for cb in ready_callbacks:
            cb()
                
    def __on_dependency(self, subtask):
        '''Called when a subtask enters one of the final states'''
        global _lock
        state = subtask.get_state()
        with _lock:
            for i, (d, cb) in enumerate(self.__dependency_callbacks):
                if d == subtask:
                    del self.__dependency_callbacks[i]
                    break
            if state == self.STATE_CANCELLED:
                self.cancel()
            elif state == self.STATE_EXCEPTION_RAISED:
                self.__finish(self.STATE_EXCEPTION_RAISED)
                self.__exception = dependency.get_exception()
            if len(self.__dependency_callbacks) == 0:
                self.__state = self.STATE_READY
                ready_callbacks = self.__ready_callbacks
                self.__ready_callbacks = []
            else:
                return
        for cb in ready_callbacks:
            cb()
        
    def __finish(self, state):
        # Assumes the lock has already been taken
        self.__state = state
        for cb in self.__callbacks:
            cb()
        self.__callbacks = []
        self.__event.set()
        
    def run(self):
        '''Run the task's function on a worker thread'''
        def set_running():
            self.__state = self.STATE_RUNNING
            
        class RunningTaskGuard(object):
            def __init__(self, task, get_state, set_running):
                self.__task = task
                self.__get_state = get_state
                self.__set_running = set_running
            def __enter__(self):
                global _lock, _thread_local, _running_tasks
                with _lock:
                    if self.__get_state() != Task.STATE_READY:
                        return False
                    if hasattr(_thread_local, _K_CURRENT_TASK):
                        self.__old_current_task = getattr(
                            _thread_local, _K_CURRENT_TASK)
                    else:
                        self.__old_current_task = None
                    setattr(_thread_local, _K_CURRENT_TASK, self.__task)
                    _running_tasks.append(self.__task)
                    self.__set_running()
                    return True
                
            def __exit__(self, exc_type, exc_value, traceback):
                global _lock, _thread_local, _running_tasks
                with _lock:
                    setattr(_thread_local, _K_CURRENT_TASK, 
                            self.__old_current_task)
                    _running_tasks.remove(self.__task)
        try:
            with RunningTaskGuard(
                self, self.get_state, set_running) as do_it:
                if do_it:
                    #
                    # Unbox the values returned by each task
                    # The unboxing will throw if there are any cancellation
                    # exceptions or regular exceptions.
                    #
                    args = tuple([
                        arg() if isinstance(arg, Task) else arg
                        for arg in self.__args])
                    kwargs = dict([
                        (k, v() if isinstance(v, Task) else v)
                        for k, v in self.__kwargs.iteritems()])
                    self.__result = self.__fn(*args, **kwargs)
        except CancellationException:
            with _lock:
                self.__finish(self.STATE_CANCELLED)
        except Exception as e:
            with _lock:
                self.__exception = e
                self.__finish(self.STATE_EXCEPTION_RAISED)
        else:
            with _lock:
                self.__finish(self.STATE_DONE)
            
        