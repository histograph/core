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
import org.waag.histograph.reasoner.TransitiveInferencer;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;

@SuppressWarnings("deprecation")
public class Main {

	Jedis jedis;
	private final String VERSION = "0.1.0";
	private final String NEO4J_PATH = "/tmp/histograph";

	public static void main(String[] argv) {
		new Main().start();
	}

	private void printAsciiArt () {
		System.out.println("    ●───────●");
		System.out.println("   /║       ║\\");
		System.out.println("  / ║       ║ \\");
		System.out.println(" ●  ║═══════║  ●    Histograph Core v" + VERSION);
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
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(NEO4J_PATH);
        
        initializeIndices(db);
        initializeServer(db);
        
        InputReader inputReader = new InputReader(db);
		List<String> messages = null;
		int messagesParsed = 0;
					
//		ExecutionEngine engine = new ExecutionEngine(db);
//		TransitiveInferencer ti = new TransitiveInferencer(db, engine);
//		ti.inferTransitiveEdges();
		
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
				writeToFile("errors.txt", "Error: ", e.getMessage());
			} catch (ConstraintViolationException e) {
				writeToFile("duplicates.txt", "Duplicate vertex: ", e.getMessage());
			}
			messagesParsed ++;
			int messagesLeft = jedis.llen("histograph-queue").intValue();
			if (messagesParsed % 100 == 0) {
				System.out.println("Parsed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
			}
		}
	}
	
	private void initializeServer (GraphDatabaseService db) {
		WrappingNeoServerBootstrapper neoServerBootstrapper;
        
        try {
        	GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        	ServerConfigurator config = new ServerConfigurator(api);
            config.configuration().addProperty(Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, "localhost");
            config.configuration().addProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, "7474");
        
            neoServerBootstrapper = new WrappingNeoServerBootstrapper(api, config);
            neoServerBootstrapper.start();
        } catch (Exception e) {
        	System.out.println("Server exception: " + e.getMessage());
        }
	}
	
	private void writeToFile(String fileName, String header, String message) {
		try {
			FileWriter fileOut = new FileWriter(fileName, true);
			fileOut.write(header + message + "\n");
			fileOut.close();
		} catch (Exception e) {
			System.out.println("Unable to write '" + message + "' to file '" + fileName + "'.");
		}	
	}
	
	private void initializeIndices (GraphDatabaseService db) {		
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
}