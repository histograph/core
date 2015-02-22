package org.waag.histograph;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.json.JSONObject;
import org.waag.histograph.queue.InputReader;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.reasoner.GraphTypes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;

public class Main {

	Jedis jedis;

	public static void main(String[] argv) {
		new Main().start();
	}

	private void initializeIndices (GraphDatabaseService db) {
		System.out.println("Initializing schema and indices...");
		
		// Create indices
		try (Transaction tx = db.beginTx()) {
			Schema schema = db.schema();
			if (!schema.getConstraints(GraphTypes.NodeType.PIT).iterator().hasNext()) {
				schema.constraintFor(GraphTypes.NodeType.PIT).assertPropertyIsUnique(NDJSONTokens.General.HGID).create();
				schema.indexFor(GraphTypes.NodeType.PIT).on(NDJSONTokens.PITTokens.NAME).create();
			}
			tx.success();
		}
		
		try (Transaction tx = db.beginTx()) {
		    Schema schema = db.schema();
		    schema.awaitIndexesOnline(10, TimeUnit.MINUTES);
		    tx.success();
		}
	}
	
	private void printAsciiArt () {
		System.out.println("    ●───────●");
		System.out.println("   /║       ║\\");
		System.out.println("  / ║       ║ \\");
		System.out.println(" ●  ║═══════║  ●    Histograph Core v0.1.0");
		System.out.println("  \\ ║       ║ /");
		System.out.println("   \\║       ║/");
		System.out.println("    ●───────●");
	}
	
	private void start() {		
		printAsciiArt();

		System.out.println("Connecting to Redis server...");
		// Initialize Redis connection
		jedis = new Jedis("localhost");
		
		try {
			jedis.ping();
		} catch (JedisConnectionException e) {
			System.out.println("Could not connect to Redis server.");
			System.exit(1);
		}
		
		System.out.println("Initializing Neo4j database...");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase("/tmp/histograph");
        InputReader inputReader = new InputReader(db);
        initializeIndices(db);

		List<String> messages = null;
		int messagesParsed = 0;
		
		try (FileWriter fileOut = new FileWriter("errors.txt", true)) {
			System.out.println("Ready to take messages.");
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
				} catch (IOException e) {
					fileOut.write("ERROR: " + e.getMessage() + "\n");
				}
				messagesParsed ++;
				int messagesLeft = jedis.llen("histograph-queue").intValue();
				if (messagesParsed % 100 == 0) {
					System.out.println("Parsed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
				}
			}
		} catch (IOException e) {
			System.out.println("Error: " + e.getMessage());
		}
	}
}
