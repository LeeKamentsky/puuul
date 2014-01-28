# (c) Copyright 2014 Broad Institute all rights reserved.
#
# Copyright under terms of the MIT license - see LICENSE for details
#
import unittest

from puuul import Task, CancellationException, check_cancel, is_inside_task_run

class TestTask(unittest.TestCase):
    def test_00_00_instantiate(self):
        Task(cmp, 0, 1)
        
    def test_01_01_run(self):
        task = Task(cmp, 0, 1)
        task.run()
        value = task()
        self.assertLess(value, 0)
        
    def test_02_01_callback_ready_run(self):
        # Test adding a callback when the task is ready
        iran = [False]
        def cb(iran = iran):
            iran[0] = True
        task = Task(cmp, 0, 1)
        task.add_callback(cb)
        self.assertFalse(iran[0])
        task.run()
        self.assertTrue(iran[0])
        
    def test_02_02_callback_run(self):
        # Test adding a callback when the task has already been run
        iran = [False]
        def cb(iran = iran):
            iran[0] = True
        task = Task(cmp, 0, 1)
        task.run()
        task.add_callback(cb)
        self.assertTrue(iran[0])
        
    def test_02_03_kwd_run(self):
        # Run a function that takes a keyword argument
        task = Task(sorted, [3, 4], cmp=(lambda a,b: cmp(a % 4, b % 4)))
        task.run()
        result = task()
        self.assertEqual(result[0], 4)
        self.assertEqual(result[1], 3)
        
    def test_03_01_blocking(self):
        task1 = Task(sum, [1, 2, 3])
        task2 = Task(sum, [4, 5, 6])
        task3 = Task(cmp, task1, task2)
        self.assertEqual(task3.get_state(), Task.STATE_BLOCKED)
        task1.run()
        self.assertEqual(task3.get_state(), Task.STATE_BLOCKED)
        task2.run()
        self.assertEqual(task3.get_state(), Task.STATE_READY)
        task3.run()
        self.assertLess(task3(), 0)
        
    def test_03_02_blocking_ready_callback(self):
        iran = [False]
        def cb(iran = iran):
            iran[0] = True
        task1 = Task(sum, [1, 2, 3])
        task2 = Task(sum, [4, 5, 6])
        task3 = Task(cmp, task1, task2)
        task1.add_ready_callback(cb)
        self.assertTrue(iran[0])
        iran[0] = False
        task3.add_ready_callback(cb)
        self.assertFalse(iran[0])
        task1.run()
        self.assertFalse(iran[0])
        task2.run()
        self.assertTrue(iran[0])

    def test_03_03_blocking_callback(self):
        iran = [False]
        def cb(iran = iran):
            iran[0] = True
        task1 = Task(sum, [1, 2, 3])
        task2 = Task(sum, [4, 5, 6])
        task3 = Task(cmp, task1, task2)
        task3.add_callback(cb)
        self.assertFalse(iran[0])
        task1.run()
        self.assertFalse(iran[0])
        task2.run()
        self.assertFalse(iran[0])
        task3.run()
        self.assertTrue(iran[0])
        
    def test_04_01_exception(self):
        class Test0401Exception(Exception):
            pass
        
        def cb():
            raise Test0401Exception()
        
        task = Task(cb)
        task.run()
        self.assertRaises(Test0401Exception, task)
        
    def test_04_02_cancel(self):
        tasks = []
        def cb(tasks=tasks):
            task = tasks[0]
            assert isinstance(task, Task)
            assert task.get_state() == Task.STATE_RUNNING
            task.cancel()
            check_cancel()
            
        task = Task(cb)
        tasks.append(task)
        task.run()
        self.assertRaises(CancellationException, task)
            
    def test_05_01_inside_task_run(self):
        iran = [False]
        def cb(iran=iran):
            self.assertTrue(is_inside_task_run())
            iran[0] = True
        
        task = Task(cb)
        task.run()
        self.assertTrue(iran[0])
        
    def test_05_02_not_inside_task_run(self):
        self.assertFalse(is_inside_task_run())