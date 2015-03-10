package org.waag.histograph;

import io.searchbox.client.JestClient;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.eclipse.jetty.util.log.Log;
import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.es.ESInit;
import org.waag.histograph.es.ESThread;
import org.waag.histograph.graph.GraphInit;
import org.waag.histograph.graph.GraphThread;
import org.waag.histograph.server.ServerThread;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.NoLogging;
import org.neo4j.graphdb.GraphDatabaseService;

public class Main {

	private static final String VERSION = "0.2.1";
	
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
		new Thread(new GraphThread(db, config.REDIS_GRAPH_QUEUE, verbose)).start();

		new Thread(new ESThread(client, config.ELASTICSEARCH_INDEX, config.ELASTICSEARCH_TYPE, config.REDIS_ES_QUEUE, verbose)).start();
		
		new Thread(new ServerThread(db, config, VERSION)).start();		
		
		List<String> messages = null;
		String payload = null;
		int messagesParsed = 0;
					
		System.out.println("[MainThread] Ready to take messages.");
		while (true) {
			try {
				messages = jedis.blpop(0, config.REDIS_MAIN_QUEUE);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				System.out.println("[MainThread] Redis connection error: " + e.getMessage());
				System.exit(1);
			}

//			System.out.println("Message received: " + payload);
						
			try {
				JSONObject obj = new JSONObject(payload);
				
				try {
					switch (obj.get(HistographTokens.General.TARGET).toString()) {
					case HistographTokens.Targets.GRAPH:
						jedis.rpush(config.REDIS_GRAPH_QUEUE, obj.toString());
						break;
					case HistographTokens.Targets.ELASTICSEARCH:
						jedis.rpush(config.REDIS_ES_QUEUE, obj.toString());
						break;
					default:
						throw new IOException("Invalid target received: " + obj.get(HistographTokens.General.ACTION).toString());
					}
				} catch (JSONException e) {
					// If the TARGET key is not found, send task to both
					jedis.rpush(config.REDIS_GRAPH_QUEUE, obj.toString());
					jedis.rpush(config.REDIS_ES_QUEUE, obj.toString());
				}
//				
//				task = InputReader.parse(obj);
//				
//				switch (task.getTarget()) {
//				case HistographTokens.Targets.ELASTICSEARCH:
//					esQueue.put(task);
//					break;
//				case HistographTokens.Targets.GRAPH:
//					graphQueue.put(task);
//					break;
//				case HistographTokens.Targets.BOTH:
//					graphQueue.put(task);
//					esQueue.put(task);
//					break;
//				default:
//					throw new IOException("Invalid action handler.");
//				}
				
				if (verbose) {
					messagesParsed ++;
					if (messagesParsed % 100 == 0) {
						int messagesLeft = jedis.llen(config.REDIS_MAIN_QUEUE).intValue();
						System.out.println("[MainThread] Distributed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
					}
				}
			} catch (JSONException e) {
				writeToFile("messageParsingErrors.txt", "JSON parse error: ", e.getMessage());
			} catch (IOException e) {
				writeToFile("messageParsingErrors.txt", "Error: ", e.getMessage());
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