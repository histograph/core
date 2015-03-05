package org.waag.histograph.graph;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.reasoner.ReasoningDefinitions;

public class GraphMethods {
	
	public static Node getVertex(GraphDatabaseService db, String hgID) throws IOException {
        try (Transaction ignored = db.beginTx()) {
        	try (ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, NDJSONTokens.General.HGID, hgID).iterator()) {
        		if (!i.hasNext()) return null;
        		Node out = i.next();
        		if (i.hasNext()) throw new IOException ("Multiple vertices with hgID '" + hgID + "' found in graph.");
        		return out;
        	}
        }
	}
	
	public static boolean vertexExists(GraphDatabaseService db, String hgID) throws IOException {
		return (getVertex(db, hgID) != null);
	}
	
	public static boolean vertexAbsent(GraphDatabaseService db, String hgID) throws IOException {
		return (getVertex(db, hgID) == null);
	}
	
	public static Relationship getEdge(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		ExecutionResult result;
		try (Transaction ignored = db.beginTx()) {
			String query = "match (p:" + ReasoningDefinitions.NodeType.PIT.toString() + " {" + NDJSONTokens.General.HGID + ": '" + escapeString(params.get(NDJSONTokens.RelationTokens.FROM)) + "'}) -[r:`" + ReasoningDefinitions.RelationType.fromLabel(params.get(NDJSONTokens.RelationTokens.LABEL)) + "`]-> (q:" + ReasoningDefinitions.NodeType.PIT.toString() + " {" + NDJSONTokens.General.HGID + ": '" + escapeString(params.get(NDJSONTokens.RelationTokens.TO)) + "'}) return r";
			result = engine.execute(query);
			Iterator<Relationship> i = result.columnAs( "r" );

			if (!i.hasNext()) return null;
			Relationship out = i.next();
			if (i.hasNext()) {
				String edgeName = params.get(NDJSONTokens.RelationTokens.FROM) + " --" + params.get(NDJSONTokens.RelationTokens.LABEL) + "--> " + params.get(NDJSONTokens.RelationTokens.TO);
				throw new IOException ("Multiple edges '" + edgeName + "' found in graph.");
			}
			return out;
		}
	}
	
	public static boolean edgeExists(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		return (getEdge(db, engine, params) != null);
	}
	
	public static boolean edgeExists(GraphDatabaseService db, ExecutionEngine engine, Node n1, Node n2, RelationshipType type) throws IOException {
		for (Relationship r : n1.getRelationships(type)) {
			if (r.getOtherNode(n1).equals(n2)) return true;
		}
		return false;
	}
	
	public static boolean edgeAbsent(GraphDatabaseService db, ExecutionEngine engine, Map<String, String> params) throws IOException {
		return (getEdge(db, engine, params) == null);
	}
	
	public static String escapeString(String input) {
		return input.replaceAll("'", "\\\\'");
	}
	
}