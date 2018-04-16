#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import threading
from collections import defaultdict
import os
import queue
from concurrent.futures import ThreadPoolExecutor  #导出线程池
from multiprocessing.pool import ThreadPool        #导出线程池
q = queue.Queue()

# 线程池
def thread_pool():
    while True:
        t, args = q.get()
        t(*args)
        q.task_done()

# 专门写文件
def task(f, lines, lock):
    # with lock:
    f.writelines(lines)


if __name__ == '__main__':
    start = time.time()
    if not os.path.exists('data'):
        os.mkdir('data')

    # 自建线程池
    for i in range(64):
        t = threading.Thread(target=thread_pool)
        t.daemon = True
        t.start()

    # 保存所有子文件指针
    fd = {}
    for i in range(1024):
        fd[i] = open('data/' + str(i) + '.txt', 'w')

    lock = threading.Lock()
    with open('data/qq.txt', 'r') as f:
        fv = defaultdict(list)
        for line in f:
            k = int(line) % 1024        # 这就是一个简单的hash函数
            fv[k].append(line)
            if len(fv[k]) >= 1000:      # 如果一个键值长度超过1000，则写入文件
                q.put((task, (fd[k], fv[k].copy(), lock)))
                fv[k].clear()
    q.join()
    # 所有队列完成后，还有部分字符长度少于1000的没有写入
    for k, f in fd.items():
        f.writelines(fv[k])
    for i in range(1024):
        fd[i].close()
    print(time.time() - start)
