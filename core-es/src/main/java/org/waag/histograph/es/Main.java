package org.waag.histograph.es;

import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class Main {
	
	Jedis jedis;
	private static final String REDIS_QUEUE = "histograph-es-queue";

	public static void main (String[] argv) {
		new Main().start();
	}
	
	public void start () {
		System.out.println("Connecting to Redis server...");
		initRedis();

		try (Node node = nodeBuilder().client(true).node()) {
			System.out.println("Connecting to Elasticsearch...");
			Client client = node.client();
			
			List<String> messages = null;
			int messagesParsed = 0;
			
			System.out.println("Ready to handle Elasticsearch messages.");
			
			while (true) {
				try {
					messages = jedis.blpop(0, REDIS_QUEUE);
				} catch (JedisConnectionException e) {
					System.out.println("Redis connection error: " + e.getMessage());
					System.exit(1);
				}
	
				String payload = messages.get(1);
				System.out.println("Message received: " + payload);
				
	//			inputReader.parse(obj);				
				
				messagesParsed ++;
				int messagesLeft = jedis.llen(REDIS_QUEUE).intValue();
				if (messagesParsed % 100 == 0) {
					System.out.println("Parsed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
				}
			}
		}
	}
	
	private void initRedis () {
		// Initialize Redis connection
		jedis = new Jedis("localhost");
		
		try {
			jedis.ping();
		} catch (JedisConnectionException e) {
			System.out.println("Could not connect to Redis server.");
			System.exit(1);
		}
	}
}
