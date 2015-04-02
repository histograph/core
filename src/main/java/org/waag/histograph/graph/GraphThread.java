package org.waag.histograph.graph;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.waag.histograph.queue.RedisInit;
import org.waag.histograph.queue.Task;
import org.waag.histograph.reasoner.AtomicInferencer;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.InputReader;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * A single thread class that keeps polling a Redis queue for graph operations and updates the graph accordingly.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class GraphThread implements Runnable {
	
	GraphDatabaseService db;
	String redis_graph_queue;
	String redis_pg_queue;
	boolean verbose;
	Jedis jedis;
	
	private static final String NAME = "GraphThread";
	
	/**
	 * Constructor of the thread.
	 * @param db The Neo4j graph object on which the operations should be performed.
	 * @param redis_graph_queue The name of the Redis queue from which the operations are pulled
	 * @param redis_pg_queue The name of the Redis queue to which the Postgres operations are pushed
	 * @param verbose Boolean value expressing whether the graph thread should write verbose output to stdout.
	 */
	public GraphThread (GraphDatabaseService db, String redis_graph_queue, String redis_pg_queue, boolean verbose) {
		this.db = db;
		this.redis_graph_queue = redis_graph_queue;
		this.redis_pg_queue = redis_pg_queue;
		this.verbose = verbose;
	}
	
	/**
	 * The thread's main loop. Keeps polling the Redis queue for tasks and performs them synchronously.
	 * Errors are written to <i>graphErrors.txt</i>, node duplicates to <i>graphDuplicates.txt</i> and Redis queue
	 * parsing errors to <i>graphMsgParseErrors.txt</i>.
	 */
	public void run () {
		try {
			jedis = RedisInit.initRedis();
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			if (verbose) e.printStackTrace();
			System.exit(1);
		}
		List<String> messages = null;
		String payload = null;
		int tasksDone = 0;
		
		namePrint("Ready to take messages.");
		while (true) {
			Task task = null;
			JSONObject message = null;
			
			try {
				messages = jedis.blpop(0, redis_graph_queue);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				namePrint("Redis connection error: " + e.getMessage());
				if (verbose) e.printStackTrace();
				System.exit(1);
			}
			
			try {
				message = new JSONObject(payload);
				task = InputReader.parse(message);
			} catch (IOException e) {
				namePrint("Error: " + e.getMessage());
				writeToFile("graphMsgParseErrors.txt", "Error: ", e.getMessage());
				continue;
			}
			
			try {
				performTask(task);
			} catch (RejectedEdgeException e) {
				passRejectedRelationToPG(message, e.getNodeHgidOrURI());
			} catch (AddedPITException e) {
				passAddedNodeToPG(message);
			} catch (IOException e) {
				writeToFile("graphErrors.txt", "Error: ", e.getMessage());
			} catch (ConstraintViolationException e) {
				writeToFile("graphDuplicates.txt", "Duplicate: ", e.getMessage());
			}
			
			if (verbose) {
				tasksDone ++;
				if (tasksDone % 100 == 0) {
					int tasksLeft = jedis.llen(redis_graph_queue).intValue();
					namePrint("Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
				}
			}
		}
	}
	
	private void passRejectedRelationToPG (JSONObject message, String cause) {
		message.put(HistographTokens.General.ACTION, HistographTokens.Actions.ADD_TO_REJECTED);
		JSONObject data = message.getJSONObject(HistographTokens.General.DATA);
		data.put(HistographTokens.RelationTokens.REJECTION_CAUSE, cause);
		
		jedis.rpush(redis_pg_queue, message.toString());	
	}
	
	private void passAddedNodeToPG (JSONObject message) {		
		jedis.rpush(redis_pg_queue, message.toString());
	}
	
	private void passRemovedRelationToPG (Map<String, String> params, String cause) {
		JSONObject message = new JSONObject();
		JSONObject data = new JSONObject();
		
		data.put(HistographTokens.RelationTokens.FROM, params.get(HistographTokens.RelationTokens.FROM));
		data.put(HistographTokens.RelationTokens.TO, params.get(HistographTokens.RelationTokens.TO));
		data.put(HistographTokens.RelationTokens.LABEL, params.get(HistographTokens.RelationTokens.LABEL));
		data.put(HistographTokens.RelationTokens.REJECTION_CAUSE, cause);
		
		message.put(HistographTokens.General.ACTION, HistographTokens.Actions.ADD_TO_REJECTED);
		message.put(HistographTokens.General.TYPE, HistographTokens.Types.RELATION);
		message.put(HistographTokens.General.SOURCE, params.get(HistographTokens.General.SOURCE));
		message.put(HistographTokens.General.DATA, data);
		
		jedis.rpush(redis_pg_queue, message.toString());	
	}
	
	private void performTask(Task task) throws IOException, AddedPITException, RejectedEdgeException {
		switch (task.getType()) {
		case HistographTokens.Types.PIT:
			performPITAction(task);
			break;
		case HistographTokens.Types.RELATION:
			performRelationAction(task);
			break;
		default:
			throw new IOException("Unexpected type received.");
		}
	}
	
	private void performPITAction(Task task) throws IOException, AddedPITException {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			GraphMethods.addNode(db, params);
			throw new AddedPITException();
		case HistographTokens.Actions.UPDATE:
			GraphMethods.updateNode(db, params);
			break;
		case HistographTokens.Actions.DELETE:
			String hgid = params.get(HistographTokens.General.HGID);
			Map<String, String>[] removedRelationMaps = GraphMethods.deleteNode(db, hgid);
			if (removedRelationMaps != null) {
				for (Map<String, String> map : removedRelationMaps) {
					passRemovedRelationToPG(map, hgid);
				}
			}
			break;
		default:
			throw new IOException("Unexpected task received.");
		}
	}
	
	private void performRelationAction(Task task) throws IOException, RejectedEdgeException {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			Relationship[] relationships = GraphMethods.addRelation(db, params);
			// If relationships are created, infer their associated atomic labels
			if (relationships != null) AtomicInferencer.inferAtomic(db, relationships);
			break;
		case HistographTokens.Actions.UPDATE:
			GraphMethods.updateRelation(db, params);
			break;
		case HistographTokens.Actions.DELETE:
			GraphMethods.deleteRelation(db, params);
			// If no relationships are removed, deleteRelation() will throw an exception. Otherwise, continue with removeInferredAtomic()
			AtomicInferencer.removeInferredAtomic(db, params);
			break;
		default:
			throw new IOException("Unexpected task received.");
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