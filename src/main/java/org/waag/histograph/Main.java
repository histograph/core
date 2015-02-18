package org.waag.histograph;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.json.JSONObject;
import org.waag.histograph.queue.CypherInputReader;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Main {

	Jedis jedis;

	public static void main(String[] argv) {
		new Main().start();
	}

//	private void initializeIndices (Client client) {
//		client.submit("g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty('name')");
//		client.submit("g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty('hgid')");
//		client.submit("g.getBaseGraph().index().getNodeAutoIndexer().setEnabled(true)");
//		
//		for (String relation : OntologyTokens.getAllRelations()) {
//			System.out.println(relation);
//			client.submit("g.getBaseGraph().index().getRelationshipAutoIndexer().startAutoIndexingProperty('" + relation + "')");
//		}
//		
//		client.submit("g.getBaseGraph().index().getRelationshipAutoIndexer().setEnabled(true)");
//	}
	
	private void start() {		
		System.out.println("Starting Histograph Core");

		// Initialize Redis connection
		jedis = new Jedis("localhost");
		
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase("/tmp/histograph");
        CypherInputReader inputReader = new CypherInputReader(db);

		List<String> messages = null;
		System.out.println("Waiting for a message in the queue");
//		int messagesParsed = 0;
		while (true) {
			try {
				messages = jedis.blpop(0, "histograph-queue");
			} catch (JedisConnectionException e) {
				System.out.println("Redis connection error: " + e.getMessage());
				System.exit(1);
			}

			String payload = messages.get(1);
//			System.out.println("Message received: " + payload);
			
			try {
				JSONObject obj = new JSONObject(payload);
				inputReader.parse(obj);				
			} catch (Exception e) {
				System.out.println("ERROR: " + e.getMessage());
			}
//			messagesParsed ++;
//			if (messagesParsed % 100 == 0) {
//				int messagesLeft = jedis.llen("histograph-queue").intValue();
//				System.out.println("Parsed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
//			}
		}
	}
}
