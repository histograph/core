package org.waag.histograph.pg;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.queue.RedisInit;
import org.waag.histograph.util.HistographTokens;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class PGThread implements Runnable {
	
	private static final String NAME = "PGThread";
	
	Connection pgconn;
	String redis_pg_queue;
	boolean verbose;
	
	public PGThread (Connection pgconn, String redis_pg_queue, boolean verbose) {
		this.pgconn = pgconn;
		this.redis_pg_queue = redis_pg_queue;
		this.verbose = verbose;
	}
	
	public void run () {
		initTables();
		
		Jedis jedis = null;
		try {
			jedis = RedisInit.initRedis();
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			System.exit(1);
		}
		List<String> messages = null;
		String payload = null;
		int tasksDone = 0;
		
		namePrint("Ready to take messages.");
		while (true) {
			JSONObject jsonMessage = null;
			
			try {
				messages = jedis.blpop(0, redis_pg_queue);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				namePrint("Redis connection error: " + e.getMessage());
				System.exit(1);
			}
			
			try {
				jsonMessage = new JSONObject(payload);
			} catch (JSONException e) {
				writeToFile("pgMsgParseErrors.txt", "Error: ", e.getMessage());
			}
			
			try {
				addToTable(jsonMessage);
			} catch (IOException e) {
				namePrint("Error: " + e.getMessage());
				writeToFile("pgErrors.txt", "Error: ", e.getMessage());
				if (verbose) {
					e.printStackTrace();
				}
			}
			
			if (verbose) {
				tasksDone ++;
				if (tasksDone % 100 == 0) {
					int tasksLeft = jedis.llen(redis_pg_queue).intValue();
					namePrint("Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
				}
			}
		}
		
	}
	
	private void addToTable(JSONObject jsonMessage) throws IOException {
		try {
			String from = jsonMessage.getString(HistographTokens.RelationTokens.FROM);
			String to = jsonMessage.getString(HistographTokens.RelationTokens.TO);
			String label = jsonMessage.getString(HistographTokens.RelationTokens.LABEL);
			String source = jsonMessage.getString(HistographTokens.General.SOURCE);
			String cause = jsonMessage.getString(HistographTokens.Types.PIT);
			
			switch (jsonMessage.getString("reason")) {
			case "not_found":
				PGMethods.addToTable(pgconn, "rejected_edges", from, to, label, source, cause);
				break;
			case "removed":
				PGMethods.addToTable(pgconn, "removed_edges", from, to, label, source, cause);
				break;
			default:
				throw new IOException("Unexpected RejectedException reason found: " + jsonMessage.get("reason"));
			}
		} catch (JSONException e) {
			throw new IOException("Could not parse JSON message", e);
		} catch (SQLException e) {
			throw new IOException("Could not add object to PostgreSQL", e);
		}
	}
	
	private void initTables () {
		try {
			if (!PGMethods.tableExists(pgconn, "rejected_edges")) {
				PGMethods.createTable(pgconn, "rejected_edges", "rel_from", "text", "rel_to", "text", "rel_label", "text", "rel_source", "text", "rejection_cause", "text");
			}
			
			if (!PGMethods.tableExists(pgconn, "removed_edges")) {
				PGMethods.createTable(pgconn, "removed_edges", "rel_from", "text", "rel_to", "text", "rel_label", "text", "rel_source", "text", "removal_cause", "text");
			}
		} catch (IOException | SQLException e) {
			System.out.println("Error in initializing PostgreSQL tables: " + e.getMessage());
			if (verbose) {
				e.printStackTrace();
			}
			System.exit(1);
		}
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