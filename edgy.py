from redis import Redis
from time import time
# print r.get('toto')

class Edgy(object):

	def __init__(self, interval, count, prefix='edgy'):
		self.interval = interval
		self.count = count
		self.prefix = prefix
		self.redis = Redis()

	def update(self, key, amount=1):
  		key = self.prefix + ':' + str(key) + ':' + str((int(time()) / self.interval + 1) * self.interval)
		res = self.redis.incr(key, amount)
		if res == amount:
			self.redis.expire(key, self.interval * self.count)

  	def get(self, key):
  		now = int(time()) / self.interval * self.interval
  		prefix = self.prefix + ':' + str(key) + ':'
  		res = 0
  		for i in self.redis.mget([prefix + str(now + i * self.interval) for i in range(2 - self.count, 2)]):
  			if i:
  				res += int(i)
  		return res

if __name__ == '__main__':
	from time import sleep
	e = Edgy(1, 5)
	while True:
		e.update(45)
		print e.get(45)
		sleep(0.1)