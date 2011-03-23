from redis import Redis
from time import time

class Edgy(object):

    def __init__(self, redis, interval, count, prefix='edgy'):
        self.redis = redis
        self.interval = interval
        self.count = count
        self.prefix = '%s-%s-%s' % (prefix, interval, count)

    def update(self, key, amount=1):
        key = self.prefix + ':' + str(key) + ':' + str(int(time()) / self.interval)
        res = self.redis.incr(key, amount)
        if res == amount:
            self.redis.expire(key, self.interval * self.count)

    def get(self, key):
        now = int(time()) / self.interval
        prefix = self.prefix + ':' + str(key) + ':'
        res = 0
        keys = [prefix + str(now + i) for i in range(1 - self.count, 1)]
        for i in self.redis.mget(keys):
            if i:
                res += int(i)
        return res

if __name__ == '__main__':
    from time import sleep
    e = Edgy(Redis(), 1, 60)
    while True:
        e.update(45)
        print e.get(45)
        sleep(0.1)