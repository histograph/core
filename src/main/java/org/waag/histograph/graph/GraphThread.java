package org.waag.histograph.graph;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.waag.histograph.reasoner.ReasoningDefinitions.RelationType;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.InputReader;
import org.waag.histograph.util.HistographTokens.PITIdentifyingMethod;

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
			
			try {
				messages = jedis.blpop(0, redis_graph_queue);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				namePrint("Redis connection error: " + e.getMessage());
				if (verbose) e.printStackTrace();
				System.exit(1);
			}
			
			try {
				JSONObject jsonMessage = new JSONObject(payload);
				task = InputReader.parse(jsonMessage);
				// If the task is a Delete Relation task, immediately forward it to PostgreSQL.
				if (task.getAction().equals(HistographTokens.Actions.DELETE) && task.getType().equals(HistographTokens.Types.RELATION)) {
					jedis.rpush(redis_pg_queue, payload);
				}
			} catch (IOException e) {
				namePrint("Error: " + e.getMessage());
				writeToFile("graphMsgParseErrors.txt", "Error: ", e.getMessage());
				continue;
			}
			
			try {
				performTask(task);
			} catch (RejectedRelationNotification n) {
				// Rejected relations may be caught after either 1) trying to add a relation, or 2) deleting a PIT.
				// This message should be forwarded to the PostgreSQL rejected relations list.
				passRejectedRelationsToPG(n.getRelationParams());
			} catch (AddedPITNotification n) {
				// After successfully adding a PIT, the added PIT is caught.
				// This should be forwarded to the PSQL rejected relations list to try rejected relations again.
				passAddedNodeToPG(payload);
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
	
	private void passRejectedRelationsToPG (Map<String, String>[] relMaps) {
		// For each of the rejected relations, construct a Redis queue message and push it to PG queue
		for (Map<String, String> params : relMaps) {			
			JSONObject message = new JSONObject();
			JSONObject data = new JSONObject();
			
			data.put(HistographTokens.RelationTokens.FROM, params.get(HistographTokens.RelationTokens.FROM));
			data.put(HistographTokens.RelationTokens.TO, params.get(HistographTokens.RelationTokens.TO));
			data.put(HistographTokens.RelationTokens.LABEL, params.get(HistographTokens.RelationTokens.LABEL));
			data.put(HistographTokens.RelationTokens.REJECTION_CAUSE, params.get(HistographTokens.RelationTokens.REJECTION_CAUSE));
			
			message.put(HistographTokens.General.ACTION, HistographTokens.Actions.ADD_TO_REJECTED);
			message.put(HistographTokens.General.TYPE, HistographTokens.Types.RELATION);
			message.put(HistographTokens.General.SOURCE, params.get(HistographTokens.General.SOURCE));
			message.put(HistographTokens.General.DATA, data);
			
			jedis.rpush(redis_pg_queue, message.toString());	
		}
	}
	
	private void passAddedNodeToPG (String message) {		
		jedis.rpush(redis_pg_queue, message);
	}
	
	private void performTask(Task task) throws IOException, AddedPITNotification, RejectedRelationNotification {
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
	
	private void performPITAction(Task task) throws IOException, AddedPITNotification, RejectedRelationNotification {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			try {
				GraphMethods.addNode(db, params);
				// Throw AddedPITNotification if adding node succeeded
				throw new AddedPITNotification();
			} catch (ConstraintViolationException e) {
				// Do nothing if adding node failed
			}
		case HistographTokens.Actions.UPDATE:
			GraphMethods.updateNode(db, params);
			break;
		case HistographTokens.Actions.DELETE:
			String hgid = params.get(HistographTokens.General.HGID);
			Map<String, String>[] removedRelationMaps = GraphMethods.deleteNode(db, hgid);
			if (removedRelationMaps != null) {
				Map<String, String>[] filteredRelationMaps = filterRemovedRelationMaps(removedRelationMaps, hgid);
				// Throw RejectedRelationNotification only if no other relations with the same parameters are found
				if (filteredRelationMaps != null) throw new RejectedRelationNotification(removedRelationMaps);
			}
			break;
		default:
			throw new IOException("Unexpected task received.");
		}
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, String>[] filterRemovedRelationMaps(Map<String, String>[] removedRelationParams, String removedPITHgid) throws IOException {
		ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
		
		for (Map<String, String> params : removedRelationParams) {
			String fromNode = params.get(HistographTokens.RelationTokens.FROM);
			PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD));
			String toNode = params.get(HistographTokens.RelationTokens.TO);
			PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD));
			RelationType relType = RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL));
			String source = params.get(HistographTokens.General.SOURCE);
			
			if (GraphMethods.relationsAbsent(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source)) {
				list.add(params);
			}
		}
		
		if (list.isEmpty()) return null;
		Map<String, String>[] out = new Map[list.size()];
		return list.toArray(out);
	}
	
	private void performRelationAction(Task task) throws IOException, RejectedRelationNotification {
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