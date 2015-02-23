package org.waag.histograph.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.queue.NDJSONTokens.RelationTokens;
import org.waag.histograph.reasoner.AtomicInferencer;
import org.waag.histograph.reasoner.GraphTypes;
import org.waag.histograph.util.GraphMethods;

public class InputReader {
	
	private GraphDatabaseService db;
	private ExecutionEngine engine;
	private static GraphMethods graphMethods;
	private static AtomicInferencer atomicInferencer;
	
	public InputReader(GraphDatabaseService db) {
		this.db = db;
		engine = new ExecutionEngine(db);
		graphMethods = new GraphMethods(db, engine);
		atomicInferencer = new AtomicInferencer(db, engine);
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
	
	private void updateEdge(JSONObject data, String layer) throws IOException {
		throw new IOException("Updating edges not supported.");
	}
	
	private void addVertex(JSONObject data, String layer) throws IOException {		
		Map<String, String> params = getVertexParams(data, layer);
		
		// Vertex lookup is omitted due to uniqueness constraint
		try (Transaction tx = db.beginTx(); ) {
			Node newPIT = db.createNode();
			newPIT.addLabel(GraphTypes.NodeType.PIT);
			
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
		if (!graphMethods.edgeAbsent(params)) { 
			String edgeName = params.get(RelationTokens.FROM + " --" + RelationTokens.LABEL + "--> " + RelationTokens.TO);
			throw new IOException ("Edge '" + edgeName + "' already exists.");
		}

		// Create edge between vertices
		try (Transaction tx = db.beginTx(); ) {
			Relationship rel = fromNode.createRelationshipTo(toNode, GraphTypes.RelationType.fromLabel(params.get(NDJSONTokens.RelationTokens.LABEL)));
			rel.setProperty(NDJSONTokens.General.LAYER, params.get(NDJSONTokens.General.LAYER));
			tx.success();
		}

		atomicInferencer.inferAtomic(params, fromNode, toNode);
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
		
		try (Transaction tx = db.beginTx(); ) {
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

		atomicInferencer.removeInferredAtomic(params);
	}
	
	private static Map<String, String> getVertexParams(JSONObject data, String layer) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			map.put(NDJSONTokens.PITTokens.NAME, data.get(NDJSONTokens.PITTokens.NAME).toString());
			map.put(NDJSONTokens.General.HGID, parseHGid(layer, data.get(NDJSONTokens.PITTokens.ID).toString()));
			map.put(NDJSONTokens.PITTokens.TYPE, data.get(NDJSONTokens.PITTokens.TYPE).toString());
			map.put(NDJSONTokens.General.LAYER, layer);
			
			if (data.has(NDJSONTokens.PITTokens.GEOMETRY)) {
				map.put(NDJSONTokens.PITTokens.GEOMETRY, data.get(NDJSONTokens.PITTokens.GEOMETRY).toString());
			}
			
			return map;
		} catch (JSONException e) {
			System.out.println("Error: " + e.getMessage());
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
		CharSequence delimiter = "/";
		if (isNumeric(id) || !id.contains(delimiter)) {
			return layer + "/" + id;
		} else {
			return id;
		}
	}
	
	private static boolean isNumeric(String string) { 
		try { 
			Integer.parseInt(string);  
		} catch(NumberFormatException e) {
			return false;  
		}  
		return true;  
	}
}