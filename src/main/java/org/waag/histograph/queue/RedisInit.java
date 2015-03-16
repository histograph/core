package org.waag.histograph.queue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisInit {

	/**
	 * Static method that initializes a new Redis connection, tests the connection and returns the Jedis object.
	 * @return A new {@link Jedis} object with a working Redis connection to localhost
	 * @throws Exception Thrown if the connection could not be made.
	 */
	public static Jedis initRedis () throws Exception {
		Jedis jedis = new Jedis("localhost");
		
		try {
			jedis.ping();
		} catch (JedisConnectionException e) {
			throw new Exception("Could not connect to Redis server.");
		}
		
		return jedis;
	}
	
}
