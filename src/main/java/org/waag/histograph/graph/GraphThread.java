package org.waag.histograph.graph;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
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
				// After successfully adding a PIT, the added PIT notification is caught.
				// This should be forwarded to the PSQL rejected relations list to try rejected relations again.
				if (n.hasURI()) {
					try {
						// If the added node has a URI, then possibly extra relations can be made
						checkForAndAddUriRelations(n);
					} catch (IOException e) {
						writeToFile("graphErrors.txt", "Error: ", e.getMessage());
					}
				}
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
	
	private void checkForAndAddUriRelations (AddedPITNotification n) throws IOException {
		// Called if a node is successfully added with a URI value.
		// It may be that existing relations, based on URI, should be added for this node as well.
		String uri = n.getURI();
		Node addedNode = n.getNode();
		if (uri == null) throw new IOException("CheckForURIs called although no URI is present in the notification.");
		
		Node[] nodesWithURI = GraphMethods.getNodesByUri(db, uri);
		if (nodesWithURI == null) throw new IOException("No nodes with URI found although it should have been added moments ago.");
		
		// No need to check if only one node is found with this URI
		if (nodesWithURI.length == 1) return;
		
		// All from node2's perspective in the situation (node1) --INCOMING--> (node2) --OUTGOING--> (node3)
		
		ArrayList<Map<String, String>> listOfNewRelations = new ArrayList<Map<String, String>>();
		
		try (Transaction tx = db.beginTx()) {
			for (Node node2 : nodesWithURI) {
				if (node2.equals(addedNode)) continue;
				Iterator<Relationship> i;
				
				i = node2.getRelationships(Direction.OUTGOING).iterator();
				while (i.hasNext()) {
					Relationship rel = i.next();
					if (rel.getProperty(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD).equals(PITIdentifyingMethod.URI.toString())) {
						PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.valueOf(rel.getProperty(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD).toString());
						
						Node otherNode = rel.getOtherNode(node2);
						String otherNodeHgidOrUri = GraphMethods.getNodePropertyByMethod(db, toIdMethod, otherNode);
						
						Node[] nodes3 = GraphMethods.getNodesByIdMethod(db, toIdMethod, otherNodeHgidOrUri);
						if (nodes3 != null) {
							for (Node node3 : nodes3) {
								Map<String, String> relParams = new HashMap<String, String>();
								
								relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
								relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
								relParams.put(HistographTokens.RelationTokens.FROM, uri);
								relParams.put(HistographTokens.RelationTokens.TO, GraphMethods.getNodePropertyByMethod(db, toIdMethod, node3));

								relParams.put(HistographTokens.RelationTokens.LABEL, RelationType.fromRelationshipType(rel.getType()).toString());
								relParams.put(HistographTokens.General.SOURCE, rel.getProperty(HistographTokens.General.SOURCE).toString());
								
								listOfNewRelations.add(relParams);
							}
						}
					}
				}
				
				i = node2.getRelationships(Direction.INCOMING).iterator();
				while (i.hasNext()) {
					Relationship rel = i.next();
					if (rel.getProperty(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD).equals(PITIdentifyingMethod.URI.toString())) {
						PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.valueOf(rel.getProperty(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD).toString());
						
						Node otherNode = rel.getOtherNode(node2);
						String otherNodeHgidOrUri = GraphMethods.getNodePropertyByMethod(db, fromIdMethod, otherNode);
						
						Node[] nodes1 = GraphMethods.getNodesByIdMethod(db, fromIdMethod, otherNodeHgidOrUri);
						if (nodes1 != null) {
							for (Node node1 : nodes1) {	
								Map<String, String> relParams = new HashMap<String, String>();
								
								relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
								relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
								relParams.put(HistographTokens.RelationTokens.TO, uri);
								relParams.put(HistographTokens.RelationTokens.FROM, GraphMethods.getNodePropertyByMethod(db, fromIdMethod, node1));

								relParams.put(HistographTokens.RelationTokens.LABEL, RelationType.fromRelationshipType(rel.getType()).toString());
								relParams.put(HistographTokens.General.SOURCE, rel.getProperty(HistographTokens.General.SOURCE).toString());
								
								listOfNewRelations.add(relParams);
							}
						}
					}
				}
			}
		}
		
		for (Map<String, String> relParams : listOfNewRelations) {
			try {
				GraphMethods.addRelation(db, relParams);
			} catch (RejectedRelationNotification e) {
				throw new IOException("Relation rejected although the nodes should have been found...!");
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
				Node node = GraphMethods.addNode(db, params);
				// Throw AddedPITNotification if adding node succeeded
				if (params.containsKey(HistographTokens.PITTokens.URI)) {
					throw new AddedPITNotification(node, params.get(HistographTokens.PITTokens.URI));
				} else {
					throw new AddedPITNotification(node);
				}
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
				if (filteredRelationMaps != null) throw new RejectedRelationNotification(filteredRelationMaps);
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