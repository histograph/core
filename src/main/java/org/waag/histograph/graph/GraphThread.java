package org.waag.histograph.graph;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.queue.NDJSONTokens.RelationTokens;
import org.waag.histograph.queue.QueueAction;
import org.waag.histograph.reasoner.AtomicInferencer;
import org.waag.histograph.reasoner.ReasoningDefinitions;

public class GraphThread implements Runnable {
	
	GraphDatabaseService db;
	ExecutionEngine engine;
	BlockingQueue<QueueAction> queue;
	boolean verbose;
	
	public GraphThread (GraphDatabaseService db, BlockingQueue<QueueAction> queue, boolean verbose) {
		this.db = db;
		engine = new ExecutionEngine(db);
		this.queue = queue;
		this.verbose = verbose;
	}
	
	public void run () {
		try {
			int actionsDone = 0;
			while (true) {
				QueueAction action = queue.take();
				
				try {
					performAction(action);
				} catch (IOException e) {
					writeToFile("graphErrors.txt", "Error: ", e.getMessage());
				} catch (ConstraintViolationException e) {
					writeToFile("graphDuplicates.txt", "Duplicate: ", e.getMessage());
				}
				
				if (verbose) {
					actionsDone ++;
					if (actionsDone % 100 == 0) {
						int actionsLeft = queue.size();
						System.out.println("[GraphThread] Processed " + actionsDone + " actions -- " + actionsLeft + " left in queue.");
					}
				}
			}
		} catch (InterruptedException e) {
			System.out.println("Graph thread interrupted!");
			System.exit(1);
		}
	}
	
	private void performAction(QueueAction action) throws IOException {
		switch (action.getType()) {
		case NDJSONTokens.Types.PIT:
			performPITAction(action);
			break;
		case NDJSONTokens.Types.RELATION:
			performRelationAction(action);
			break;
		default:
			throw new IOException("Unexpected type received.");
		}
	}
	
	private void performPITAction(QueueAction action) throws IOException {
		Map<String, String> params = action.getParams();
		switch (action.getAction()) {
		case NDJSONTokens.Actions.ADD:
			addVertex(params);
			break;
		case NDJSONTokens.Actions.UPDATE:
			updateVertex(params);
			break;
		case NDJSONTokens.Actions.DELETE:
			deleteVertex(params);
			break;
		default:
			throw new IOException("Unexpected action received.");
		}
	}
	
	private void performRelationAction(QueueAction action) throws IOException {
		Map<String, String> params = action.getParams();
		switch (action.getAction()) {
		case NDJSONTokens.Actions.ADD:
			addEdge(params);
			break;
		case NDJSONTokens.Actions.UPDATE:
			updateEdge(params);
			break;
		case NDJSONTokens.Actions.DELETE:
			deleteEdge(params);
			break;
		default:
			throw new IOException("Unexpected action received.");
		}
	}
	
	private void addVertex(Map<String, String> params) throws IOException {		
		// Vertex lookup is omitted due to uniqueness constraint
		try (Transaction tx = db.beginTx()) {
			Node newPIT = db.createNode();
			newPIT.addLabel(ReasoningDefinitions.NodeType.PIT);
			
			for (Entry <String,String> entry : params.entrySet()) {
				newPIT.setProperty(entry.getKey(), entry.getValue());
			}
			
			tx.success();
		}
	}
	
	private void updateVertex(Map<String, String> params) throws IOException {
		// Verify vertex exists and get it
		Node node = GraphMethods.getVertex(db, params.get(NDJSONTokens.General.HGID));
		if (node == null) throw new IOException ("Vertex " + params.get(NDJSONTokens.General.HGID) + " not found in graph.");
		
		// Update vertex
		try (Transaction tx = db.beginTx()) {
			// Remove all old properties
			for (String key : node.getPropertyKeys()) {
				node.removeProperty(key);
			}
			
			// Add all new properties
			for (Entry<String, String> entry : params.entrySet()) {
				node.setProperty(entry.getKey(), entry.getValue());
			}
			
			tx.success();
		}
		
		System.out.println("Vertex updated.");
	}
	
	private void deleteVertex(Map<String, String> params) throws IOException {
		String hgid = params.get(NDJSONTokens.General.HGID);
		
		// Verify vertex exists and get it
		Node node = GraphMethods.getVertex(db, hgid);
		if (node == null) throw new IOException("Vertex with hgid '" + hgid + "' not found in graph.");
		
		try (Transaction tx = db.beginTx()) {
			// Remove all associated relationships
			Iterable<Relationship> relationships = node.getRelationships();
			for (Relationship rel : relationships) {
				rel.delete();
			}
			
			// Remove node
			node.delete();
			tx.success();
		}
	}
	
	private void addEdge(Map<String, String> params) throws IOException {		
		// Verify both vertices exist and get them
		Node fromNode = GraphMethods.getVertex(db, params.get(NDJSONTokens.RelationTokens.FROM));
		if (fromNode == null) throw new IOException("Vertex with hgid " + params.get(NDJSONTokens.RelationTokens.FROM) + " not found in graph.");

		Node toNode = GraphMethods.getVertex(db, params.get(NDJSONTokens.RelationTokens.TO));		
		if (toNode == null) throw new IOException("Vertex with hgid " + params.get(NDJSONTokens.RelationTokens.TO) + " not found in graph.");		

		// Verify the absence of the new edge
		if (!GraphMethods.edgeAbsent(db, engine, params)) { 
			String edgeName = params.get(RelationTokens.FROM + " --" + RelationTokens.LABEL + "--> " + RelationTokens.TO);
			throw new IOException ("Edge '" + edgeName + "' already exists.");
		}

		// Create edge between vertices
		try (Transaction tx = db.beginTx()) {
			Relationship rel = fromNode.createRelationshipTo(toNode, ReasoningDefinitions.RelationType.fromLabel(params.get(NDJSONTokens.RelationTokens.LABEL)));
			rel.setProperty(NDJSONTokens.General.LAYER, params.get(NDJSONTokens.General.LAYER));
			tx.success();
		}

		AtomicInferencer.inferAtomic(db, engine, params, fromNode, toNode);
	}
	
	private void updateEdge(Map<String, String> params) throws IOException {
		throw new IOException("Updating edges not supported.");
	}
	
	private void deleteEdge(Map<String, String> params) throws IOException {
		// Verify edge exists and get it
		Relationship rel = GraphMethods.getEdge(db, engine, params); 
		if (rel == null) throw new IOException("Edge not found in graph.");
		
		// Remove edge
		try (Transaction tx = db.beginTx()) {
			rel.delete();
			tx.success();
		}

		AtomicInferencer.removeInferredAtomic(db, engine, params);
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