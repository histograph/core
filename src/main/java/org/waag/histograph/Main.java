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
import org.waag.histograph.util.Configuration;
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

	private static final String VERSION = "0.1.1";
	
	Jedis jedis;
	static Configuration config;

	public static void main(String[] argv) {
		boolean fromFile = false;
		boolean logging = false;
		
		for (int i=0; i<argv.length; i++) {
			if (argv[i].equals("-v") || argv[i].equals("-verbose")) {
				logging = true;
			}
			if (argv[i].equals("-config")) {
				try {
					config = Configuration.fromFile(argv[i+1]);
					fromFile = true;
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println("Error: No config file provided.");
					System.exit(1);
				} catch (IOException e) {
					System.out.println("Error: " + e.getMessage());
					System.exit(1);
				}
			}
		}
		
		if (!logging) {
			disableLogging();
		}
		
		if (!fromFile) {
			try {
				config = Configuration.fromEnv();
			} catch (IOException e) {
				System.out.println("Error: " + e.getMessage());
				System.exit(1);
			}
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
		
		System.out.println("Initializing Neo4j server...");
		GraphDatabaseService db = initNeo4j();		
		
		System.out.println("Initializing Neo4j thread...");
		initNeo4jThread(db);
		
		System.out.println("Initializing Elasticsearch thread...");
		initESThread();
		
        InputReader inputReader = new InputReader(db);
		List<String> messages = null;
		String payload = null;
		int messagesParsed = 0;
					
		System.out.println("Ready to take messages.");
		while (true) {
			try {
				messages = jedis.blpop(0, config.REDIS_HISTOGRAPH_QUEUE);
				payload = messages.get(1);
				jedis.rpush(config.REDIS_ES_QUEUE, payload);
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
			int messagesLeft = jedis.llen(config.REDIS_HISTOGRAPH_QUEUE).intValue();
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
	
	private GraphDatabaseService initNeo4j () {
		try {
			GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(config.NEO4J_FILEPATH);
	        initializeIndices(db);
	        initializeServer(db);
	        return db;
		} catch (RuntimeException e) {
			System.out.println("Unable to start graph database on " + config.NEO4J_FILEPATH + ".");
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	private void initializeServer (GraphDatabaseService db) {
		WrappingNeoServerBootstrapper neoServerBootstrapper;
        
        try {
        	GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        	ServerConfigurator serverConfig = new ServerConfigurator(api);
            serverConfig.configuration().addProperty(Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, "localhost");
            serverConfig.configuration().addProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, config.NEO4J_PORT);
        
            neoServerBootstrapper = new WrappingNeoServerBootstrapper(api, serverConfig);
            neoServerBootstrapper.start();
        } catch (Exception e) {
        	System.out.println("Server exception: " + e.getMessage());
        }
        
        System.out.println("Neo4j listening on http://localhost:" + config.NEO4J_PORT + "/");
	}
	
	private void initNeo4jThread(GraphDatabaseService db) {
		
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
	
	private void initESThread () {
		
	}
	
	private static void disableLogging () {
		Logger globalLogger = Logger.getLogger("");
		globalLogger.setLevel(java.util.logging.Level.OFF);
		Log.setLog(new NoLogging());
	}
}