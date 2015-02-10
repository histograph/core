package org.waag.histograph;

import java.util.Iterator;
import java.util.List;

import redis.clients.jedis.Jedis;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.driver.ResultSet;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.Settings;

public class Main {
	
	Neo4jGraph g;
	GremlinServer s;
	Jedis jedis;
	
	public static void main(String[] argv) {
		new Main().start();
	}

	private void start() {
		System.out.println("Starting Histograph Core");
				
		try {
			Settings settings = Settings.read(getClass().getResourceAsStream("/gremlin-server-neo4j.yaml"));
			s = new GremlinServer(settings);			
			s.run();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		jedis = new Jedis("localhost");
		
		Cluster cluster = Cluster.open();
		Client client = cluster.connect(); 
		
		List<String> messages = null;
        while(true){
          System.out.println("Waiting for a message in the queue");
          messages = jedis.blpop(0, "histograph-queue");
          
          // messages.get(1) is JSON object:
          // {
          //  "action": ["add", "delete", "update"],
          //  "type": ["vertex", "edge"],
          //  "data": {}
          // }
          
          
          System.out.println("Got the message");
          System.out.println("KEY:" + messages.get(0) + " VALUE:" + messages.get(1));
          String payload = messages.get(1);
          //Do some processing with the payload
          System.out.println("Message received:" + payload);
          
          // Als er is dinges dan doe iets!
          //ResultSet results = client.submit("g.V().label()");
        }
				
	
		

		
		// 3. Start Gremlin Server
		// 4. En blijf lekker draaien!
		// 5. Haal neo4j-dir en andere toestanden uit config-bestand!
		
		
		
		
	}
	
}
