package org.waag.histograph;

import io.searchbox.client.JestClient;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.eclipse.jetty.util.log.Log;
import org.json.JSONObject;
import org.waag.histograph.es.ESInit;
import org.waag.histograph.es.ESThread;
import org.waag.histograph.graph.GraphInit;
import org.waag.histograph.graph.GraphThread;
import org.waag.histograph.queue.InputReader;
import org.waag.histograph.queue.QueueAction;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.NoLogging;
import org.neo4j.graphdb.GraphDatabaseService;

public class Main {

	private static final String VERSION = "0.2.0";
	
	static Configuration config;
	private static boolean verbose;

	public static void main(String[] argv) {
		boolean fromFile = false;
		verbose = false;
		
		for (int i=0; i<argv.length; i++) {
			if (argv[i].equals("-v") || argv[i].equals("-verbose")) {
				verbose = true;
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
		
		if (!verbose) {
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
		Jedis jedis = initRedis();
		
		System.out.println("Initializing Elasticsearch client...");
		JestClient client = ESInit.initES(config);
		
		System.out.println("Initializing Neo4j server...");
		GraphDatabaseService db = GraphInit.initNeo4j(config);
		
		System.out.println("Creating threads...");
		BlockingQueue<QueueAction> graphQueue = new LinkedBlockingQueue<QueueAction>();		
		new Thread(new GraphThread(db, graphQueue, verbose)).start();

		BlockingQueue<QueueAction> esQueue = new LinkedBlockingQueue<QueueAction>();
		new Thread(new ESThread(client, esQueue, config.ELASTICSEARCH_INDEX, config.ELASTICSEARCH_TYPE, verbose)).start();
		
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
			
			QueueAction action = null;
			
			try {
				JSONObject obj = new JSONObject(payload);
				action = InputReader.parse(obj);
				
				switch (action.getHandler()) {
				case ELASTICSEARCH:
					esQueue.put(action);
					break;
				case GRAPH:
					graphQueue.put(action);
					break;
				case BOTH:
					graphQueue.put(action);
					esQueue.put(action);
					break;
				default:
					throw new IOException("Invalid action handler.");
				}
				
				if (verbose) {
					messagesParsed ++;
					if (messagesParsed % 100 == 0) {
						int messagesLeft = jedis.llen(config.REDIS_HISTOGRAPH_QUEUE).intValue();
						System.out.println("[MainThread] Parsed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
					}
				}
			} catch (IOException e) {
				writeToFile("messageParsingErrors.txt", "Error: ", e.getMessage());
			} catch (InterruptedException e) {
				System.out.println("Caught interrupted exception: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	private Jedis initRedis () {
		// Initialize Redis connection
		Jedis jedis = new Jedis("localhost");
		
		try {
			jedis.ping();
		} catch (JedisConnectionException e) {
			System.out.println("Could not connect to Redis server.");
			System.exit(1);
		}
		
		return jedis;
	}
	
	private static void disableLogging () {
		Logger globalLogger = Logger.getLogger("");
		globalLogger.setLevel(java.util.logging.Level.OFF);
		Log.setLog(new NoLogging());
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
}