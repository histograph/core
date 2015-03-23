package org.waag.histograph;

import io.searchbox.client.JestClient;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.eclipse.jetty.util.log.Log;
import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.es.ESInit;
import org.waag.histograph.es.ESMethods;
import org.waag.histograph.es.ESThread;
import org.waag.histograph.graph.GraphInit;
import org.waag.histograph.graph.GraphThread;
import org.waag.histograph.queue.RedisInit;
import org.waag.histograph.traversal.ServerThread;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.NoLogging;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Main Histograph-Core program. This program starts several threads for each of the Core's functionalities.
 * One is created for graph management, one for Elasticsearch communication and one for the traversal server.
 * The Main thread keeps polling the Redis queue for actions and distributes them to the appropriate threads.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class Main {

	private static final String VERSION = "0.2.2";
	
	static Configuration config;
	private static boolean verbose;
	private static final String NAME = "MainThread";

	/**
	 * Initiates the Histograph-Core program.
	 * @param argv Two arguments are allowed: <i>-verbose</i> for verbose output, and <i>-config</i> when a
	 * configuration path is provided. Both are optional -- omitting the <i>-config</i> argument results in
	 * the program trying to read the configuration file path from the environment variable <i>HISTOGRAPH_CONFIG</i>.
	 */
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
		
		namePrint("Connecting to Redis server...");
		Jedis jedis = null;
		try {
			jedis = RedisInit.initRedis();
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			System.exit(1);
		}
			
		namePrint("Initializing Elasticsearch client...");
		JestClient client = null;
		try {
			client = ESInit.initES(config);
			if (!ESMethods.indexExists(client, config)) {
				ESMethods.createIndex(config);
			}
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			System.exit(1);
		}
		
		namePrint("Initializing Neo4j server...");
		GraphDatabaseService db = null;
		try {
			db = GraphInit.initNeo4j(config);
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
			System.exit(1);
		}
		
		namePrint("Creating threads...");
		new Thread(new GraphThread(db, config.REDIS_GRAPH_QUEUE, verbose)).start();
		new Thread(new ESThread(client, config, verbose)).start();
		new Thread(new ServerThread(db, config, VERSION)).start();		
		
		List<String> messages = null;
		String payload = null;
		int messagesParsed = 0;
					
		namePrint("Ready to take messages.");
		while (true) {
			try {
				messages = jedis.blpop(0, config.REDIS_MAIN_QUEUE);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				namePrint("Redis connection error: " + e.getMessage());
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
				
				if (verbose) {
					messagesParsed ++;
					if (messagesParsed % 100 == 0) {
						int messagesLeft = jedis.llen(config.REDIS_MAIN_QUEUE).intValue();
						namePrint("Distributed " + messagesParsed + " messages -- " + messagesLeft + " left in queue.");
					}
				}
			} catch (JSONException e) {
				writeToFile("messageParsingErrors.txt", "JSON parse error: ", e.getMessage());
			} catch (IOException e) {
				writeToFile("messageParsingErrors.txt", "Error: ", e.getMessage());
			}
		}
	}
	
	private static void disableLogging () {
		Logger globalLogger = Logger.getLogger("");
		globalLogger.setLevel(java.util.logging.Level.OFF);
		Log.setLog(new NoLogging());
	}
	
	private void writeToFile(String fileName, String header, String message) {
		try {	
			FileWriter fileOut = new FileWriter(fileName, true);
			Date now = new Date();
			SimpleDateFormat format = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss] ");
			fileOut.write(format.format(now) + header + message + "\n");
			fileOut.close();
		} catch (Exception e) {
			namePrint("Unable to write '" + message + "' to file '" + fileName + "'.");
		}	
	}
	
	private void namePrint(String message) {
		System.out.println("[" + NAME + "] " + message);
	}
}