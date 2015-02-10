package org.waag.histograph;

import java.io.IOException;
import java.util.List;

import redis.clients.jedis.Jedis;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.Settings;

import org.json.*;
import org.waag.histograph.queue.InputReader;

public class Main {
	
	GremlinServer s;
	Jedis jedis;
	
	public static void main(String[] argv) {
		new Main().start();
	}

	private void start() {
		System.out.println("Starting Histograph Core");
				
		try {
			Settings settings = Settings.read(this.getClass().getResourceAsStream("/gremlin-server-neo4j.yaml"));
			System.out.println(settings.graphs.get("g"));
			s = new GremlinServer(settings);			
			s.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		jedis = new Jedis("localhost");
		
		Cluster cluster = Cluster.open();
		Client client = cluster.connect(); 
		
		List<String> messages = null;
        while (true) {
          System.out.println("Waiting for a message in the queue");
          messages = jedis.blpop(0, "histograph-queue");
          
          // messages.get(1) is JSON object:
          // {
          //  "action": ["add", "delete", "update"],
          //  "type": ["vertex", "edge"],
          //  "data": {}
          // }
          
          String payload = messages.get(1);
          System.out.println("Message received: " + payload);
          
          try {
        	  JSONObject obj = new JSONObject(payload);
        	  InputReader.parse(obj, client);
          } catch (JSONException | IOException e) {
        	  System.out.println("Error: " + e.getMessage());
          }
          
          // Als er is dinges dan doe iets!
//          ResultSet results = client.submit("g.V().label()");
        }
				
	
		

		
		// 3. Start Gremlin Server
		// 4. En blijf lekker draaien!
		// 5. Haal neo4j-dir en andere toestanden uit config-bestand!
		
		
		
		
	}
	
}
