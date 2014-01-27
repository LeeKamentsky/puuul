import unittest

from puuul import ThreadPool, check_cancel
from puuul import CancellationException

class TestPuuul(unittest.TestCase):
    def test_01_01_start_and_stop(self):
        pool = ThreadPool()
        pool.start()
        pool.stop()
        
    def test_01_02_enter_exit(self):
        with ThreadPool() as pool:
            pass
        
    def test_02_01_do(self):
        with ThreadPool() as pool:
            task = pool.do(sum, [1, 1])
            self.assertEqual(task(), 2)
            
    def test_02_02_do_subtasks(self):
        with ThreadPool() as pool:
            task = pool.do(cmp, pool.do(sum, [1, 2]), pool.do(sum, [3, 4]))
            self.assertLess(task(), 0)
            
    def test_02_03_raise(self):
        def fn():
            raise Exception()
        with ThreadPool() as pool:
            task = pool.do(fn)
            self.assertRaises(Exception, task)
            
    def test_03_01_cancel(self):
        def fn():
            while(True):
                check_cancel()
                
        with ThreadPool() as pool:
            task = pool.do(fn)
            task.cancel()
            self.assertRaises(CancellationException, task)