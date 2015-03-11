package org.waag.histograph.queue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisInit {

	public static Jedis initRedis () {
		// Initialize Redis connection
		Jedis jedis = new Jedis("localhost");
		
		try {
			jedis.ping();
		} catch (JedisConnectionException e) {
			System.out.println("Could not connect to Redis server.");
			System.exit(1);
		}
		
		return jedis;
	}
	
}
