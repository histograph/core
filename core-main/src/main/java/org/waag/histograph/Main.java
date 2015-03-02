package org.waag.histograph;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.eclipse.jetty.util.log.Log;
import org.json.JSONObject;
import org.waag.histograph.queue.InputReader;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.reasoner.GraphDefinitions;
import org.waag.histograph.util.NoLogging;
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

	private static final String VERSION = "0.1.0";
	private static final String NEO4J_PATH = "/tmp/histograph";
	
	private static final String IMPORT_QUEUE = "histograph-queue";
	private static final String ES_QUEUE = "histograph-es-queue";
	
	private static final String WEBSERVER_ADDRESS = "localhost";
	private static final String WEBSERVER_PORT = "7474";
	
	Jedis jedis;
	GraphDatabaseService db;

	public static void main(String[] argv) {
		if (!(argv.length > 0 && argv[0].equals("-v"))) {
			disableLogging();
		}
			
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
		initRedis();
		
		System.out.println("Initializing Neo4j database...");
		initNeo4j();
        
        InputReader inputReader = new InputReader(db);
		List<String> messages = null;
		String payload = null;
		int messagesParsed = 0;
					
		System.out.println("Ready to take messages.");
		while (true) {
			try {
				messages = jedis.blpop(0, IMPORT_QUEUE);
				payload = messages.get(1);
				jedis.rpush(ES_QUEUE, payload);
			} catch (JedisConnectionException e) {
				System.out.println("Redis connection error: " + e.getMessage());
				System.exit(1);
			}

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
			int messagesLeft = jedis.llen(IMPORT_QUEUE).intValue();
			if (messagesParsed % 100 == 0) {
				System.out.println("Parsed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
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
	
	private void initNeo4j () {		
		try {
			db = new GraphDatabaseFactory().newEmbeddedDatabase(NEO4J_PATH);
		} catch (RuntimeException e) {
			System.out.println("Unable to start graph database on " + NEO4J_PATH + ".");
			e.printStackTrace();
			System.exit(1);
		}
		
        initializeIndices(db);
        initializeServer(db);
	}
	
	private void initializeServer (GraphDatabaseService db) {
		WrappingNeoServerBootstrapper neoServerBootstrapper;
        
        try {
        	GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        	ServerConfigurator config = new ServerConfigurator(api);
            config.configuration().addProperty(Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, WEBSERVER_ADDRESS);
            config.configuration().addProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, WEBSERVER_PORT);
        
            neoServerBootstrapper = new WrappingNeoServerBootstrapper(api, config);
            neoServerBootstrapper.start();
        } catch (Exception e) {
        	System.out.println("Server exception: " + e.getMessage());
        }
        
        System.out.println("Neo4j listening on http://" + WEBSERVER_ADDRESS + ":" + WEBSERVER_PORT + "/");
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
			if (!schema.getConstraints(GraphDefinitions.NodeType.PIT).iterator().hasNext()) {
				schema.constraintFor(GraphDefinitions.NodeType.PIT).assertPropertyIsUnique(NDJSONTokens.General.HGID).create();
				schema.indexFor(GraphDefinitions.NodeType.PIT).on(NDJSONTokens.PITTokens.NAME).create();
			}
			tx.success();
		}
		
		try (Transaction tx = db.beginTx()) {
		    Schema schema = db.schema();
		    schema.awaitIndexesOnline(10, TimeUnit.MINUTES);
		    tx.success();
		}
	}
	
	private static void disableLogging () {
		Logger globalLogger = Logger.getLogger("");
		globalLogger.setLevel(java.util.logging.Level.OFF);
		Log.setLog(new NoLogging());
	}
}