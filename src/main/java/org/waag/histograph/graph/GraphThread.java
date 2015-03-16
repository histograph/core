package org.waag.histograph.graph;

import java.io.FileWriter;
import java.io.IOException;
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
	boolean verbose;
	private static final String NAME = "GraphThread";
	
	/**
	 * Constructor of the thread.
	 * @param db The Neo4j graph object on which the operations should be performed.
	 * @param redis_graph_queue The name of the Redis queue from which the operations are pulled
	 * @param verbose Boolean value expressing whether the graph thread should write verbose output to stdout.
	 */
	public GraphThread (GraphDatabaseService db, String redis_graph_queue, boolean verbose) {
		this.db = db;
		this.redis_graph_queue = redis_graph_queue;
		this.verbose = verbose;
	}
	
	/**
	 * The thread's main loop. Keeps polling the Redis queue for tasks and performs them synchronously.
	 * Errors are written to <i>graphErrors.txt</i>, node duplicates to <i>graphDuplicates.txt</i> and Redis queue
	 * parsing errors to <i>graphMsgParseErrors.txt</i>.
	 */
	public void run () {
		Jedis jedis = null;
		try {
			jedis = RedisInit.initRedis();
		} catch (Exception e) {
			println("Error: " + e.getMessage());
			System.exit(1);
		}
		List<String> messages = null;
		String payload = null;
		int tasksDone = 0;
		
		println("Ready to take messages.");
		while (true) {
			Task task = null;
			
			try {
				messages = jedis.blpop(0, redis_graph_queue);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				println("Redis connection error: " + e.getMessage());
				System.exit(1);
			}
			
			try {
				JSONObject jsonMessage = new JSONObject(payload);
				task = InputReader.parse(jsonMessage);
			} catch (IOException e) {
				writeToFile("graphMsgParseErrors.txt", "Error: ", e.getMessage());
			}
			
			try {
				performTask(task);
			} catch (IOException e) {
				writeToFile("graphErrors.txt", "Error: ", e.getMessage());
			} catch (ConstraintViolationException e) {
				writeToFile("graphDuplicates.txt", "Duplicate: ", e.getMessage());
			}
			
			if (verbose) {
				tasksDone ++;
				if (tasksDone % 100 == 0) {
					int tasksLeft = jedis.llen(redis_graph_queue).intValue();
					println("Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
				}
			}
		}
	}
	
	private void performTask(Task task) throws IOException {
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
	
	private void performPITAction(Task task) throws IOException {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			GraphMethods.addNode(db, params);
			break;
		case HistographTokens.Actions.UPDATE:
			GraphMethods.updateNode(db, params);
			break;
		case HistographTokens.Actions.DELETE:
			GraphMethods.deleteNode(db, params);
			break;
		default:
			throw new IOException("Unexpected task received.");
		}
	}
	
	private void performRelationAction(Task task) throws IOException {
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
			fileOut.write(header + message + "\n");
			fileOut.close();
		} catch (Exception e) {
			println("Unable to write '" + message + "' to file '" + fileName + "'.");
		}	
	}
	
	private void println(String message) {
		System.out.println("[" + NAME + "] " + message);
	}
}