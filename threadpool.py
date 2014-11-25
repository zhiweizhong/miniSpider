#/***************************************************************************
#* 
#* Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#* 
#**************************************************************************/
"""
This module is a threadpool with message queue

 * @file threadpool.py
 * @author zhongzhiwei01
 * @date 2014/07/28 10:46:33
 * @brief 
 * 
"""


#!/usr/bin/python
#-*- coding: utf-8 -*-


import Queue
import threading


class ThreadPool(object):
    """
    The thread pool class.
    """
    def __init__(self, thread_num):
        self.work_queue = Queue.Queue()
        self.thread_pool = []
        self.thread_num = thread_num

    def thread_start(self):
        """
        Start the thread.
        """
        for t in range(0, int(self.thread_num)):
            self.thread_pool.append(Work(self.work_queue))

    def add_job(self, func, *args, **kargs):
        """
        Add work job to the thread pool.
        """
        self.work_queue.put((func, args, kargs))


class Work(threading.Thread):
    """
    Work    - do the real job.

    Attributes:
        work_queue:the real work queue.
    """
    def __init__(self, work_queue):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.work_queue = work_queue
        self.state = None
        self.start()

    def run(self):
        """
        the get-some-work, do-some-work main loop of worker threads.
        """
        while True:
            if self.state == 'stop':
                break
            fun, args, kargs = self.work_queue.get()
            fun(*args, **kargs)
            self.work_queue.task_done()


