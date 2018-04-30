package l2.thrift.cache.implementations.redis.client;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisPoolClientImpl extends RedisClientImpl {
	private JedisPool jedisPool;

	public RedisPoolClientImpl(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

	@Override
	protected Jedis getResource() {
		return jedisPool.getResource();
	}

}
