#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import Counter
import time
import multiprocessing as mp


def find(start):
    d = []
    for name in range(start, start + 2 ** 4):
        with open('data/' + str(name) + '.txt', 'r') as f:
            d.extend(Counter(f.readlines()).most_common(10))
    return d


if __name__ == '__main__':
    t = time.time()

    # 利用Manager传递变量
    # p = mp.Pool(16)
    # manager = mp.Manager()
    # d = manager.list()
    # for i in range(0, 2 ** 10, 2 ** 4):
    #     p.apply_async(find, args=(i,d))
    # p.close()
    # p.join()

    # 获取进程的返回值
    res = []
    d = []
    p = mp.Pool()
    for i in range(0, 2 ** 10, 2 ** 6):   # 每个进程处理64个文件
        res.append(p.apply_async(find, args=(i,)))
    p.close()
    p.join()
    for r in res:
        d.extend(r.get())
    print(Counter(dict(d)).most_common(10))
    print(time.time() - t)
