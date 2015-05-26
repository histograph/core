package org.waag.histograph;

import io.searchbox.client.JestClient;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.es.ESInit;
import org.waag.histograph.es.ESMethods;
import org.waag.histograph.es.ESThread;
import org.waag.histograph.graph.GraphInit;
import org.waag.histograph.graph.GraphThread;
import org.waag.histograph.pg.PGInit;
import org.waag.histograph.pg.PGThread;
import org.waag.histograph.queue.RedisInit;
import org.waag.histograph.server.ServerThread;
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

	private final String VERSION = getClass().getPackage().getImplementationVersion();
	private static final String NAME = "MainThread";
	
	static Configuration config;
	private static boolean verbose;

	/*
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
				if (verbose) {
					e.printStackTrace();
				}
				System.exit(1);
			}
		}
			
		new Main().start();
	}

	private void printAsciiArt () {
		JSONArray logo = config.LOGO;
		
		int nLines = logo.length();
		boolean even = (nLines % 2 == 0);
		int middleLine = even ? (nLines/2)-1 : (nLines-1)/2;
		
		for (int i=0; i<nLines; i++) {
			String line = logo.get(i).toString();			
			if (i==middleLine) line += "   Histograph Core v" + VERSION;
			System.out.println(line);
		}
	}
	
	private void start() {
		printAsciiArt();
		
		namePrint("Connecting to Redis server...");
		Jedis jedis = null;
		try {
			jedis = RedisInit.initRedis();
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			if (verbose) {
				e.printStackTrace();
			}
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
			if (verbose) e.printStackTrace();
			System.exit(1);
		}
		
		namePrint("Initializing PostgreSQL connection...");
		Connection pg = null;
		try {
			pg = PGInit.initPG(config);
		} catch (SQLException e) {
			namePrint("Could not initialize PostgreSQL connection. " + e.getMessage());
			if (verbose) e.printStackTrace();
			System.exit(1);
		}
		
		namePrint("Initializing Neo4j server...");
		GraphDatabaseService db = null;
		try {
			db = GraphInit.initNeo4j(config);
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			if (verbose) e.printStackTrace();
			System.exit(1);
		}
		
		namePrint("Creating threads...");
		new Thread(new GraphThread(db, config.REDIS_GRAPH_QUEUE, config.REDIS_PG_QUEUE, verbose)).start();
		new Thread(new ESThread(client, config, verbose)).start();
		new Thread(new PGThread(pg, config, verbose)).start();
		new Thread(new ServerThread(db, pg, config, VERSION, verbose)).start();		
		
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
				if (verbose) e.printStackTrace();
				System.exit(1);
			}
						
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
					// If the TARGET key is not found (JSONException thrown), send task to both Graph and ES
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
		// silence the root logger
		java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.OFF);

		// disable jetty logging by passing a no-op logger
		org.eclipse.jetty.util.log.Log.setLog(new NoLogging());

		// finally, tell io.searchbox to stfu
		final org.slf4j.Logger l = org.slf4j.LoggerFactory.getLogger("io.searchbox");
		if (l instanceof ch.qos.logback.classic.Logger)
			((ch.qos.logback.classic.Logger) l).setLevel(ch.qos.logback.classic.Level.OFF);
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
			if (verbose) e.printStackTrace();
		}	
	}
	
	private void namePrint(String message) {
		System.out.println("[" + NAME + "] " + message);
	}
}