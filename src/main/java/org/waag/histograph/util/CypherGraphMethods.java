package org.waag.histograph.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.reasoner.GraphTypes;

public class CypherGraphMethods {
	
	private GraphDatabaseService db;
	private ExecutionEngine engine;
	
	public CypherGraphMethods (GraphDatabaseService db, ExecutionEngine engine) {
		this.db = db;
		this.engine = engine;
	}
	
	public Node getVertex(String hgID) throws IOException {
        try (Transaction ignored = db.beginTx()) {
        	try (ResourceIterator<Node> i = db.findNodesByLabelAndProperty(GraphTypes.NodeType.PIT, NDJSONTokens.General.HGID, hgID).iterator()) {
        		if (!i.hasNext()) return null;
        		Node out = i.next();
        		if (i.hasNext()) throw new IOException ("Multiple vertices with hgID '" + hgID + "' found in graph.");
        		return out;
        	}
        }
	}
	
	public boolean vertexExists(String hgID) throws IOException {
		return (getVertex(hgID) != null);
	}
	
	public boolean vertexAbsent(String hgID) throws IOException {
		return (getVertex(hgID) == null);
	}
	
	public Relationship getEdge(Map<String, String> params) throws IOException {
		ExecutionResult result;
		try (Transaction ignored = db.beginTx()) {
			String query = "match (p:" + GraphTypes.NodeType.PIT.toString() + " {" + NDJSONTokens.General.HGID + ": '" + escapeString(params.get(NDJSONTokens.RelationTokens.FROM)) + "'}) -[r:`" + GraphTypes.RelationType.fromLabel(params.get(NDJSONTokens.RelationTokens.LABEL)) + "`]-> (q:" + GraphTypes.NodeType.PIT.toString() + " {" + NDJSONTokens.General.HGID + ": '" + escapeString(params.get(NDJSONTokens.RelationTokens.TO)) + "'}) return r";
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
	
	public boolean edgeExists(Map<String, String> params) throws IOException {
		return (getEdge(params) != null);
	}
	
	public boolean edgeAbsent(Map<String, String> params) throws IOException {
		return (getEdge(params) == null);
	}
	
	public String escapeString(String input) {
		return input.replaceAll("'", "\\\\'");
	}
	
}