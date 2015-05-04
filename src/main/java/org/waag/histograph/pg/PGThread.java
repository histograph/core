package org.waag.histograph.pg;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.queue.RedisInit;
import org.waag.histograph.queue.Task;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.InputReader;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class PGThread implements Runnable {
	
	private static final String NAME = "PGThread";
	private static final String TABLE_NAME = "rejected_relations";
	
	private Connection pg;
	private Configuration config;
	private boolean verbose;
	private Jedis jedis;
	
	public PGThread (Connection pg, Configuration config, boolean verbose) {
		this.pg = pg;
		this.config = config;
		this.verbose = verbose;
	}
	
	public void run () {
		initTable(TABLE_NAME);
		initIndex(TABLE_NAME);
		
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
			Task task = null;
			
			try {
				messages = jedis.blpop(0, config.REDIS_PG_QUEUE);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				namePrint("Redis connection error: " + e.getMessage());
				System.exit(1);
			}
			
			try {
				JSONObject message = new JSONObject(payload);
				task = InputReader.parse(message);
			} catch (IOException e) {
				namePrint("Error: " + e.getMessage());
				writeToFile("graphMsgParseErrors.txt", "Error: ", e.getMessage());
				continue;
			}
			
			try {
				performTask(task);
			} catch (IOException e) {
				namePrint("Error: " + e.getMessage());
				writeToFile("pgErrors.txt", "Error: ", e.getMessage());
				if (verbose) e.printStackTrace();
			}
			
			if (verbose) {
				tasksDone ++;
				if (tasksDone % 100 == 0) {
					int tasksLeft = jedis.llen(config.REDIS_PG_QUEUE).intValue();
					namePrint("Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
				}
			}
		}
		
	}
	
	private void performTask(Task task) throws IOException {
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			if (task.getType().equals(HistographTokens.Types.PIT)) {
				parseAddedPIT(task);
			} else {
				throw new IOException("Unexpected type received with ADD action: " + task.getType());
			}
			break;
		case HistographTokens.Actions.ADD_TO_REJECTED:
			if (task.getType().equals(HistographTokens.Types.RELATION)) {
				parseRejectedRelation(task);
			} else {
				throw new IOException("Unexpected type received with ADD action: " + task.getType());
			}
			break;
		case HistographTokens.Actions.DELETE:
			if (task.getType().equals(HistographTokens.Types.RELATION)) {
				parseDeletedRelation(task);
			}
			break;
		default:
			throw new IOException("Unexpected action received: " + task.getAction());
		}
	}
	
	private void parseDeletedRelation(Task task) throws IOException {
		Map<String, String> params = task.getParams();

		String from = params.get(HistographTokens.RelationTokens.FROM);
		String fromIdMethod = params.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD);
		String to = params.get(HistographTokens.RelationTokens.TO);
		String toIdMethod = params.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD);
		String label = params.get(HistographTokens.RelationTokens.LABEL);
		String source = params.get(HistographTokens.General.SOURCEID);
		
		try {
			PGMethods.deleteFromTable(pg, TABLE_NAME, "rel_from", from, "from_id_method", fromIdMethod, "rel_to", to, "to_id_method", toIdMethod, "rel_label", label, "rel_source", source);
		} catch (SQLException e) {
			throw new IOException("Unexpected response from PG:", e);
		}
	}
	
	private void parseAddedPIT(Task task) throws IOException {
		Map<String, String> params = task.getParams();
		
		String hgid = params.get(HistographTokens.General.HGID);
		findRejectedRelationsHgid(hgid);
		
		if (params.containsKey(HistographTokens.PITTokens.URI)) {
			String uri = params.get(HistographTokens.PITTokens.URI);
			findRejectedRelationsURI(uri);
		}
	}
	
	private void findRejectedRelationsHgid(String hgid) throws IOException {
		try {
			Map<String, String>[] relationParams = PGMethods.getRowsByKeyValPairs(pg, TABLE_NAME, "rejection_cause_id_method", "HGID", "rejection_cause", hgid);
			if (relationParams != null) {
				for (Map<String, String> map : relationParams) {
					pushRelationToGraph(map);
				}
				PGMethods.deleteFromTable(pg, TABLE_NAME, "rejection_cause_id_method", "HGID", "rejection_cause", hgid);
			}
		} catch (SQLException e) {
			throw new IOException("Error in PSQL communication: ", e);
		}
	}
	
	private void findRejectedRelationsURI(String uri) throws IOException {
		try {
			Map<String, String>[] relationParams = PGMethods.getRowsByKeyValPairs(pg, TABLE_NAME, "rejection_cause_id_method", "URI", "rejection_cause", uri);
			if (relationParams != null) {
				for (Map<String, String> map : relationParams) {
					pushRelationToGraph(map);
				}
				PGMethods.deleteFromTable(pg, TABLE_NAME, "rejection_cause_id_method", "URI", "rejection_cause", uri);
			}
		} catch (SQLException e) {
			throw new IOException("Error in PSQL communication: ", e);
		}
	}
	
	private void pushRelationToGraph(Map<String, String> relParams) {
		JSONObject graphMessage = new JSONObject();
		JSONObject data = new JSONObject();
		
		// Construct message and push to Graph
		graphMessage.put(HistographTokens.General.ACTION, HistographTokens.Actions.ADD);
		graphMessage.put(HistographTokens.General.TYPE, HistographTokens.Types.RELATION);
		graphMessage.put(HistographTokens.General.SOURCEID, relParams.get("rel_source"));
		
		data.put(HistographTokens.RelationTokens.FROM, relParams.get("rel_from"));
		data.put(HistographTokens.RelationTokens.TO, relParams.get("rel_to"));
		data.put(HistographTokens.RelationTokens.LABEL, relParams.get("rel_label"));
		
		graphMessage.put(HistographTokens.General.DATA, data);
		
		jedis.rpush(config.REDIS_GRAPH_QUEUE, graphMessage.toString());
	}
	
	private void parseRejectedRelation(Task task) throws IOException {
		try {
			Map<String, String> params = task.getParams();
						
			String from = params.get(HistographTokens.RelationTokens.FROM);
			String fromIdMethod = params.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD);
			String to = params.get(HistographTokens.RelationTokens.TO);
			String toIdMethod = params.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD);
			String label = params.get(HistographTokens.RelationTokens.LABEL);
			String source = params.get(HistographTokens.General.SOURCEID);
			String cause = params.get(HistographTokens.RelationTokens.REJECTION_CAUSE);
			String causeIdMethod = params.get(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD);
			
			if (!PGMethods.rowExists(pg, TABLE_NAME, from, fromIdMethod, to, toIdMethod, label, source, cause, causeIdMethod)) {
				PGMethods.addToTable(pg, TABLE_NAME, from, fromIdMethod, to, toIdMethod, label, source, cause, causeIdMethod);	
			}
			
		} catch (JSONException | SQLException e) {
			throw new IOException("Error in PSQL communication: ", e);
		}
	}
	
	private void initTable (String tableName) {
		try {
			if (!PGMethods.tableExists(pg, tableName)) {
				PGMethods.createTable(pg, tableName, "rel_from", "text", "from_id_method", "text", "rel_to", "text", "to_id_method", "text", "rel_label", "text", "rel_source", "text", "rejection_cause", "text", "rejection_cause_id_method", "text");
			}
		} catch (IOException | SQLException e) {
			System.out.println("Error in initializing PostgreSQL tables: " + e.getMessage());
			if (verbose) e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void initIndex (String tableName) {
		String[] rejected_edges_cols = null;
		try {
			rejected_edges_cols = PGMethods.getColumnNames(pg, tableName);
		} catch (SQLException | IOException e) {
			namePrint("Error in getting table columns: " + e.getMessage());
			if (verbose) e.printStackTrace();
		}
		
		for (String col : rejected_edges_cols) {
			if (!PGMethods.indexExists(pg, tableName, col)) {
				try {
					PGMethods.createIndex(pg, tableName, col);
				} catch (IOException | SQLException e) {
					namePrint("Error in creating index: " + e.getMessage());
					if (verbose) e.printStackTrace();
				}
			}
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
			if (verbose) e.printStackTrace();
		}	
	}
	
	private void namePrint(String message) {
		System.out.println("[" + NAME + "] " + message);
	}
}