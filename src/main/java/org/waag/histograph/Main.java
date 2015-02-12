package org.waag.histograph;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import redis.clients.jedis.Jedis;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.server.GremlinServer;

import org.json.JSONException;
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

		//		try {
		//			Settings settings = Settings.read(this.getClass().getResourceAsStream("/gremlin-server-neo4j.yaml"));
		//			System.out.println(settings.graphs.get("g"));
		//			s = new GremlinServer(settings);			
		//			s.run();
		//		} catch (Exception e) {
		//			e.printStackTrace();
		//		}

		jedis = new Jedis("localhost");

		Cluster cluster = Cluster.open();
		Client client = cluster.connect(); 

		List<String> messages = null;
		System.out.println("Waiting for a message in the queue");
		while (true) {
			messages = jedis.blpop(0, "histograph-queue");

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
