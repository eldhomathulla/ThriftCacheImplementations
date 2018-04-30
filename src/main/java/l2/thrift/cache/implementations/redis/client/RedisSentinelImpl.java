package l2.thrift.cache.implementations.redis.client;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class RedisSentinelImpl extends RedisClientImpl {
	private JedisSentinelPool jedisSentinelPool;

	public RedisSentinelImpl(JedisSentinelPool jedisSentinelPool) {
		this.jedisSentinelPool = jedisSentinelPool;
	}

	@Override
	protected Jedis getResource() {
		return jedisSentinelPool.getResource();
	}

}
