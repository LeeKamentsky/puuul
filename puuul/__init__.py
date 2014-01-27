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
from ._puuul import ThreadPool, start, stop, do
from ._cancellation_exception import CancellationException
from ._task import Task, check_cancel



