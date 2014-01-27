puuul (pronounced Pool)
=====

Python thread pool and execution graph

This library implements a framework for managing a fixed-size thread pool
whose threads pull tasks from a queue and execute them. The tasks wrap
a function and its arguments into a callable object. Tasks perform a primitive
unboxing of arguments - if a positional or keyword argument is itself a task,
then the parent task will remain in a blocking state until the subtask
has been run. The library is best suited for long-running calculations that
release the Global Interpreter Lock (GIL) or for blocking I/O that does
the same.

The task framework captures the return value from a function, the exception
raised during function execution or a cancellation exception if the task
was cancelled. A caller can either issue a blocking call for the task value
(which will reraise a raised exception) or can provide a callback that will
be called when the task has been executed.

An example

import puuul

puuul.start()
t1 = puuul.do(sum, [1, 2])
t2 = puuul.do(sum, [3, 4], start=1)
t3 = puuul.do(cmp, t1, t2)
print t3() # prints -1
print t2() # prints 1+3+4 = 8

# or more simply

print puuul.do(cmp, puuul.do(sum, [1, 2]), puuul.do(sum, [3, 4], start = 1))