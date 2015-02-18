package org.waag.histograph;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.json.JSONObject;
import org.waag.histograph.queue.CypherInputReader;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.reasoner.OntologyTokens;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Main {

	Jedis jedis;

	public static void main(String[] argv) {
		new Main().start();
	}

	private void initializeIndices (GraphDatabaseService db) {
		System.out.println("Initializing indices...");
		db.index().getNodeAutoIndexer().startAutoIndexingProperty(NDJSONTokens.PITTokens.NAME);
		db.index().getNodeAutoIndexer().startAutoIndexingProperty(NDJSONTokens.General.HGID);
		db.index().getNodeAutoIndexer().setEnabled(true);
		
		System.out.println("Auto-indexed node properties:");
		for (String s : db.index().getNodeAutoIndexer().getAutoIndexedProperties()) {
			System.out.println(s);
		};
		
		for (String relation : OntologyTokens.getAllRelations()) {
			db.index().getRelationshipAutoIndexer().startAutoIndexingProperty(relation);
		}
		
		db.index().getRelationshipAutoIndexer().setEnabled(true);
		
		System.out.println("Auto-indexed relationship properties:");
		for (String s : db.index().getRelationshipAutoIndexer().getAutoIndexedProperties()) {
			System.out.println(s);
		};
	}
	
	private void start() {		
		System.out.println("Starting Histograph Core");

		// Initialize Redis connection
		jedis = new Jedis("localhost");
		
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase("/tmp/histograph");
        CypherInputReader inputReader = new CypherInputReader(db);
        
        initializeIndices(db);

		List<String> messages = null;
		System.out.println("Waiting for a message in the queue");
		int messagesParsed = 0;
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
			messagesParsed ++;
			if (messagesParsed % 100 == 0) {
				int messagesLeft = jedis.llen("histograph-queue").intValue();
				System.out.println("Parsed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
			}
		}
	}
}
