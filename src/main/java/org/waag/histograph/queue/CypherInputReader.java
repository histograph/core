package org.waag.histograph.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.reasoner.CypherAtomicInferencer;
import org.waag.histograph.util.CypherGraphMethods;

public class CypherInputReader {
	
	private GraphDatabaseService db;
	private ExecutionEngine engine;
	private static CypherGraphMethods graphMethods;
	private static CypherAtomicInferencer atomicInferencer;
	
	public CypherInputReader(GraphDatabaseService db) {
		this.db = db;
		engine = new ExecutionEngine(db);
		graphMethods = new CypherGraphMethods(db, engine);
		atomicInferencer = new CypherAtomicInferencer(db, engine);
	}
	
	public void parse(JSONObject obj) throws IOException {
		String layer;
		
		try {
			layer = obj.get(NDJSONTokens.General.LAYER).toString();
			switch (obj.get(NDJSONTokens.General.ACTION).toString()) {
			case NDJSONTokens.Actions.ADD:
				parseAdd(obj, layer);
				break;
			case NDJSONTokens.Actions.DELETE:
				parseDelete(obj, layer);
				break;
			case NDJSONTokens.Actions.UPDATE:
				parseUpdate(obj, layer);
				break;
			default:
				throw new IOException("Invalid action received: " + obj.get(NDJSONTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No layer or action in JSON input.");
		}	
	}
	
	private void parseAdd(JSONObject obj, String layer) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				addVertex(data, layer);
				break;
			case NDJSONTokens.Types.RELATION:
				addEdge(data, layer);
				break;
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private void parseDelete(JSONObject obj, String layer) throws IOException {		
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				deleteVertex(data, layer);
				break;
			case NDJSONTokens.Types.RELATION:
				deleteEdge(data, layer);
				break;
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private void parseUpdate(JSONObject obj, String layer) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				updateVertex(data, layer);
				break;
			case NDJSONTokens.Types.RELATION:
				updateEdge(data, layer);
				break;
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private void updateVertex(JSONObject data, String layer) throws IOException {
		Map<String, String> params = getVertexParams(data, layer);

		// Verify vertex exists and get it
		Node node = graphMethods.getVertex(params.get(NDJSONTokens.General.HGID));
		if (node == null) throw new IOException ("Vertex " + params.get(NDJSONTokens.General.HGID) + " not found in graph.");
		
		// Update vertex
		try (Transaction tx = db.beginTx(); ) {
			node.setProperty(NDJSONTokens.PITTokens.NAME, params.get(NDJSONTokens.PITTokens.NAME));
			node.setProperty(NDJSONTokens.PITTokens.TYPE, params.get(NDJSONTokens.PITTokens.TYPE));
			tx.success();
		}
		
		System.out.println("Vertex updated.");
	}
	
	private void updateEdge(JSONObject data, String layer) throws IOException {
		throw new IOException("Updating edges not supported.");
	}
	
	private void addVertex(JSONObject data, String layer) throws IOException {		
		Map<String, String> params = getVertexParams(data, layer);
		
		if (!graphMethods.vertexAbsent(params.get(NDJSONTokens.General.HGID))) throw new IOException ("Vertex " + params.get(NDJSONTokens.General.HGID) + " already exists.");
		
		try (Transaction tx = db.beginTx(); ) {
			Node newPIT = db.createNode();
			
			for (Entry <String,String> entry : params.entrySet()) {
				newPIT.setProperty(entry.getKey(), entry.getValue());
			}
			
			tx.success();
		}
	}
	
	private void addEdge(JSONObject data, String layer) throws IOException {
		Map<String, String> params = getEdgeParams(data, layer);
		
		// Verify both vertices exist and get them
		Node fromNode = graphMethods.getVertex(params.get(NDJSONTokens.RelationTokens.FROM));
		if (fromNode == null) throw new IOException("Vertex with hgID " + params.get(NDJSONTokens.RelationTokens.FROM) + " not found in graph.");
		
		Node toNode = graphMethods.getVertex(params.get(NDJSONTokens.RelationTokens.TO));
		if (toNode == null) throw new IOException("Vertex with hgID " + params.get(NDJSONTokens.RelationTokens.TO) + " not found in graph.");		
		
		// Verify the absence of the new edge
		if (!graphMethods.edgeAbsent(params)) throw new IOException ("Edge already exists.");
		
		// Create edge between vertices
		try (Transaction tx = db.beginTx(); ) {
			RelationshipType relType = DynamicRelationshipType.withName(params.get(NDJSONTokens.RelationTokens.LABEL));
			Relationship rel = fromNode.createRelationshipTo(toNode, relType);
			rel.setProperty(NDJSONTokens.General.LAYER, params.get(NDJSONTokens.General.LAYER));
			tx.success();
		}
		
		atomicInferencer.inferAtomic(params);
	}

	private void deleteVertex(JSONObject data, String layer) throws IOException {
		String hgID;
		try {
			hgID = parseHGid(layer, data.get(NDJSONTokens.PITTokens.ID).toString());
		} catch (JSONException e) {
			throw new IOException("Vertex ID missing.");
		}
		
		// Verify vertex exists and get it
		Node node = graphMethods.getVertex(hgID);
		if (node == null) throw new IOException("Vertex with hgID " + hgID + " not found in graph.");

//		int relCounter = 0;
		
		try (Transaction tx = db.beginTx(); ) {
			// Remove all relationships
			Iterable<Relationship> relationships = node.getRelationships();
			for (Relationship rel : relationships) {
				rel.delete();
//				relCounter++;
			}
			
			// Remove node
			node.delete();
			tx.success();
		}
//		System.out.println("Vertex successfully deleted -- " + relCounter + " edges removed in the process.");
	}
	
	private void deleteEdge(JSONObject data, String layer) throws IOException {
		Map<String, String> params = getEdgeParams(data, layer);

		// Verify edge exists and get it
		Relationship rel = graphMethods.getEdge(params); 
		if (rel == null) throw new IOException("Edge not found in graph.");
		
		// Remove edge
		try (Transaction tx = db.beginTx(); ) {
			rel.delete();
			tx.success();
		}

//		String edgeLabel = params.get(NDJSONTokens.RelationTokens.FROM) + "--" + params.get(NDJSONTokens.RelationTokens.LABEL) + "-->" + params.get(NDJSONTokens.RelationTokens.TO);
//		System.out.println("Edge " + edgeLabel + " successfully deleted.");
		
		atomicInferencer.removeInferredAtomic(params);
	}
	
	private static Map<String, String> getVertexParams(JSONObject data, String layer) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			map.put(NDJSONTokens.PITTokens.NAME, data.get(NDJSONTokens.PITTokens.NAME).toString());
			map.put(NDJSONTokens.General.HGID, parseHGid(layer, data.get(NDJSONTokens.PITTokens.ID).toString()));
			map.put(NDJSONTokens.PITTokens.TYPE, data.get(NDJSONTokens.PITTokens.TYPE).toString());
			map.put(NDJSONTokens.General.LAYER, layer);
			return map;
		} catch (JSONException e) {
			throw new IOException("Vertex token(s) missing (" + NDJSONTokens.PITTokens.ID + "/" + NDJSONTokens.PITTokens.NAME + "/" + NDJSONTokens.PITTokens.TYPE + ").");
		}
	}
	
	private static Map<String, String> getEdgeParams(JSONObject data, String layer) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			map.put(NDJSONTokens.RelationTokens.FROM, parseHGid(layer, data.get(NDJSONTokens.RelationTokens.FROM).toString()));
			map.put(NDJSONTokens.RelationTokens.TO, parseHGid(layer, data.get(NDJSONTokens.RelationTokens.TO).toString()));
			map.put(NDJSONTokens.RelationTokens.LABEL, data.get(NDJSONTokens.RelationTokens.LABEL).toString());
			map.put(NDJSONTokens.General.LAYER, layer);
			return map;
		} catch (JSONException e) {
			throw new IOException("Edge token(s) missing (" + NDJSONTokens.RelationTokens.FROM + "/" + NDJSONTokens.RelationTokens.TO + "/" + NDJSONTokens.RelationTokens.LABEL + ").");
		}
	}
	
	private static String parseHGid (String layer, String id) {
		if (!id.startsWith(layer + "/")) {
			return layer + "/" + id;
		} else {
			return id;
		}
	}
}