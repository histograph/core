package org.waag.histograph.graph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.reasoner.ReasoningDefinitions;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.HistographTokens.RelationTokens;

public class GraphMethods {
	
	public static void addNode(GraphDatabaseService db, Map<String, String> params) throws IOException {		
		// Node lookup is omitted due to uniqueness constraint
		try (Transaction tx = db.beginTx()) {
			Node newPIT = db.createNode();
			newPIT.addLabel(ReasoningDefinitions.NodeType.PIT);
			
			for (Entry <String,String> entry : params.entrySet()) {
				newPIT.setProperty(entry.getKey(), entry.getValue());
			}
			
			tx.success();
		}
	}
	
	public static void updateNode(GraphDatabaseService db, Map<String, String> params) throws IOException {
		// Verify node exists and get it
		Node node = GraphMethods.getNode(db, params.get(HistographTokens.General.HGID));
		if (node == null) throw new IOException ("Node with " + HistographTokens.General.HGID + " '" + params.get(HistographTokens.General.HGID) + "' not found in graph.");
		
		// Update node
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
		
		System.out.println("Node updated.");
	}
	
	public static void deleteNode(GraphDatabaseService db, Map<String, String> params) throws IOException {
		String hgid = params.get(HistographTokens.General.HGID);
		
		// Verify node exists and get it
		Node node = GraphMethods.getNode(db, hgid);
		if (node == null) throw new IOException("Node with " + HistographTokens.General.HGID + " '" + hgid + "' not found in graph.");
		
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
	
	public static Node[] addRelation(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {		
		// Verify both nodes exist and get them
		Node fromNode = GraphMethods.getNode(db, params.get(HistographTokens.RelationTokens.FROM));
		if (fromNode == null) throw new IOException("Node with " + HistographTokens.General.HGID + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' not found in graph.");

		Node toNode = GraphMethods.getNode(db, params.get(HistographTokens.RelationTokens.TO));		
		if (toNode == null) throw new IOException("Node with " + HistographTokens.General.HGID + " '" + params.get(HistographTokens.RelationTokens.TO) + "' not found in graph.");		

		// Verify the absence of the new relation
		if (!GraphMethods.relationAbsent(db, engine, params)) { 
			String relationName = params.get(RelationTokens.FROM + " --" + RelationTokens.LABEL + "--> " + RelationTokens.TO);
			throw new IOException("Relation '" + relationName + "' already exists.");
		}

		// Create relation between nodes
		try (Transaction tx = db.beginTx()) {
			Relationship rel = fromNode.createRelationshipTo(toNode, ReasoningDefinitions.RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL)));
			rel.setProperty(HistographTokens.General.LAYER, params.get(HistographTokens.General.LAYER));
			tx.success();
		}
		
		// Return the two nodes
		Node[] nodes = new Node[2];
		nodes[0] = fromNode;
		nodes[1] = toNode;
		return nodes;
	}
	
	public static void updateRelation(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		throw new IOException("Updating relations not supported.");
	}
	
	public static void deleteRelation(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		// Verify relation exists and get it
		Relationship rel = GraphMethods.getRelation(db, engine, params); 
		if (rel == null) throw new IOException("Relation not found in graph.");
		
		// Remove relation
		try (Transaction tx = db.beginTx()) {
			rel.delete();
			tx.success();
		}
	}
	
	public static Node getNode(GraphDatabaseService db, String hgID) throws IOException {
        try (Transaction ignored = db.beginTx()) {
        	try (ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, HistographTokens.General.HGID, hgID).iterator()) {
        		if (!i.hasNext()) return null;
        		Node out = i.next();
        		if (i.hasNext()) throw new IOException ("Multiple nodes with " + HistographTokens.General.HGID + " '" + hgID + "' found in graph.");
        		return out;
        	}
        }
	}
	
	public static boolean nodeExists(GraphDatabaseService db, String hgID) throws IOException {
		return (getNode(db, hgID) != null);
	}
	
	public static boolean nodeAbsent(GraphDatabaseService db, String hgID) throws IOException {
		return (getNode(db, hgID) == null);
	}
	
	public static Relationship getRelation(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		ExecutionResult result;
		try (Transaction ignored = db.beginTx()) {
			String query = "match (p:" + ReasoningDefinitions.NodeType.PIT.toString() + " {" + HistographTokens.General.HGID + ": '" + escapeString(params.get(HistographTokens.RelationTokens.FROM)) + "'}) -[r:`" + ReasoningDefinitions.RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL)) + "`]-> (q:" + ReasoningDefinitions.NodeType.PIT.toString() + " {" + HistographTokens.General.HGID + ": '" + escapeString(params.get(HistographTokens.RelationTokens.TO)) + "'}) return r";
			result = engine.execute(query);
			Iterator<Relationship> i = result.columnAs( "r" );

			if (!i.hasNext()) return null;
			Relationship out = i.next();
			if (i.hasNext()) {
				String relationName = params.get(HistographTokens.RelationTokens.FROM) + " --" + params.get(HistographTokens.RelationTokens.LABEL) + "--> " + params.get(HistographTokens.RelationTokens.TO);
				throw new IOException ("Multiple relations '" + relationName + "' found in graph.");
			}
			return out;
		}
	}
	
	public static boolean relationExists(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		return (getRelation(db, engine, params) != null);
	}
	
	public static boolean relationExists(GraphDatabaseService db, ExecutionEngine engine, Node n1, Node n2, RelationshipType type) throws IOException {
		for (Relationship r : n1.getRelationships(type)) {
			if (r.getOtherNode(n1).equals(n2)) return true;
		}
		return false;
	}
	
	public static boolean relationAbsent(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		return (getRelation(db, engine, params) == null);
	}
	
	public static String escapeString(String input) {
		return input.replaceAll("'", "\\\\'");
	}
	
}