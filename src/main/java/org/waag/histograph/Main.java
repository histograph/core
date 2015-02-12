package org.waag.histograph;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.server.GremlinServer;

import org.json.JSONObject;
import org.waag.histograph.queue.InputReader;

public class Main {

	GremlinServer s;
	Jedis jedis;

	public static void main(String[] argv) {
		new Main().start();
		System.out.println("Starting Histograph Core");
	}

	private void start() {

		// Initialize Redis connection
		jedis = new Jedis("localhost");

		// Initialize Gremlin Server connection
		Cluster cluster = Cluster.open();
		Client client = cluster.connect(); 

		List<String> messages = null;
		System.out.println("Waiting for a message in the queue");
		while (true) {
			try {
				messages = jedis.blpop(0, "histograph-queue");
			} catch (JedisConnectionException e) {
				System.out.println("Redis connection error: " + e.getMessage());
				System.exit(1);
			}

			String payload = messages.get(1);
			System.out.println("Message received: " + payload);

			try {
				JSONObject obj = new JSONObject(payload);
				InputReader.parse(obj, client);
			} catch (Exception e) {
				System.out.println("Error: " + e.getMessage());
			}
		}
	}
}
