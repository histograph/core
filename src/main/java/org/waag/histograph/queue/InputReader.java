package org.waag.histograph.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;

public class InputReader {
	
	public static QueueAction parse(JSONObject obj) throws IOException {
		String layer;
		ActionHandler handler;
		
		try {
			switch (obj.get(NDJSONTokens.General.TARGET).toString()) {
			case NDJSONTokens.Targets.GRAPH:
				handler = ActionHandler.GRAPH;
				break;
			case NDJSONTokens.Targets.ELASTICSEARCH:
				handler = ActionHandler.ELASTICSEARCH;
				break;
			default:
				throw new IOException("Invalid target received: " + obj.get(NDJSONTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			handler = ActionHandler.BOTH;
		}
		
		try {
			layer = obj.get(NDJSONTokens.General.LAYER).toString();
			switch (obj.get(NDJSONTokens.General.ACTION).toString()) {
			case NDJSONTokens.Actions.ADD:
				return parseAdd(obj, layer, handler);
			case NDJSONTokens.Actions.DELETE:
				return parseDelete(obj, layer, handler);
			case NDJSONTokens.Actions.UPDATE:
				return parseUpdate(obj, layer, handler);
			default:
				throw new IOException("Invalid action received: " + obj.get(NDJSONTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No layer or action in JSON input.");
		}	
	}
	
	private static QueueAction parseAdd(JSONObject obj, String layer, ActionHandler handler) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				return addVertex(data, layer, handler);
			case NDJSONTokens.Types.RELATION:
				return addEdge(data, layer, handler);
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static QueueAction parseDelete(JSONObject obj, String layer, ActionHandler handler) throws IOException {		
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				return deleteVertex(data, layer, handler);
			case NDJSONTokens.Types.RELATION:
				return deleteEdge(data, layer, handler);
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static QueueAction parseUpdate(JSONObject obj, String layer, ActionHandler handler) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				return updateVertex(data, layer, handler);
			case NDJSONTokens.Types.RELATION:
				return updateEdge(data, layer, handler);
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static QueueAction addVertex(JSONObject data, String layer, ActionHandler handler) throws IOException {		
		Map<String, String> params = getVertexParams(data, layer);		
		return new QueueAction(handler, NDJSONTokens.Types.PIT, NDJSONTokens.Actions.ADD, params);
	}
	
	private static QueueAction updateVertex(JSONObject data, String layer, ActionHandler handler) throws IOException {
		Map<String, String> params = getVertexParams(data, layer);
		return new QueueAction(handler, NDJSONTokens.Types.PIT, NDJSONTokens.Actions.UPDATE, params);
	}
	
	private static QueueAction deleteVertex(JSONObject data, String layer, ActionHandler handler) throws IOException {
		try {
			String hgid = parseHGid(layer, data.get(NDJSONTokens.PITTokens.ID).toString());
			Map<String, String> params = new HashMap<String, String>();
			params.put(NDJSONTokens.General.HGID, hgid);
			return new QueueAction(handler, NDJSONTokens.Types.PIT, NDJSONTokens.Actions.DELETE, params);
		} catch (JSONException e) {
			throw new IOException("Vertex ID missing.");
		}
	}
	
	private static QueueAction addEdge(JSONObject data, String layer, ActionHandler handler) throws IOException {
		Map<String, String> params = getEdgeParams(data, layer);
		return new QueueAction(handler, NDJSONTokens.Types.RELATION, NDJSONTokens.Actions.ADD, params);
	}
	
	private static QueueAction updateEdge(JSONObject data, String layer, ActionHandler handler) throws IOException {
		throw new IOException("Updating edges not supported.");
	}
	
	private static QueueAction deleteEdge(JSONObject data, String layer, ActionHandler handler) throws IOException {
		Map<String, String> params = getEdgeParams(data, layer);
		return new QueueAction(handler, NDJSONTokens.Types.RELATION, NDJSONTokens.Actions.DELETE, params);
	}
	
	private static Map<String, String> getVertexParams(JSONObject data, String layer) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			map.put(NDJSONTokens.PITTokens.NAME, data.get(NDJSONTokens.PITTokens.NAME).toString());
			map.put(NDJSONTokens.General.HGID, parseHGid(layer, data.get(NDJSONTokens.PITTokens.ID).toString()));
			map.put(NDJSONTokens.PITTokens.TYPE, data.get(NDJSONTokens.PITTokens.TYPE).toString());
			map.put(NDJSONTokens.General.LAYER, layer);
			
			// Optional predefined tokens
			if (data.has(NDJSONTokens.PITTokens.GEOMETRY)) {
				map.put(NDJSONTokens.PITTokens.GEOMETRY, data.get(NDJSONTokens.PITTokens.GEOMETRY).toString());
			}
			if (data.has(NDJSONTokens.PITTokens.STARTDATE)) {
				map.put(NDJSONTokens.PITTokens.STARTDATE, data.get(NDJSONTokens.PITTokens.STARTDATE).toString());
			}
			if (data.has(NDJSONTokens.PITTokens.ENDDATE)) {
				map.put(NDJSONTokens.PITTokens.ENDDATE, data.get(NDJSONTokens.PITTokens.ENDDATE).toString());
			}
			if (data.has(NDJSONTokens.PITTokens.URI)) {
				map.put(NDJSONTokens.PITTokens.URI, data.get(NDJSONTokens.PITTokens.URI).toString());
			}
			
			// Other specific data, add as single string
			if (data.has(NDJSONTokens.PITTokens.DATA)) {
				map.put(NDJSONTokens.PITTokens.DATA, data.getJSONObject(NDJSONTokens.PITTokens.DATA).toString());
			}
			
			// Remove keys with empty values
			Iterator<Entry<String, String>> iter = map.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, String> entry = iter.next();
				if (entry.getValue().equals("")) iter.remove();
			}
			
			// Validate existence of compulsory keys
			if (!map.containsKey(NDJSONTokens.PITTokens.NAME)) throw new IOException("Vertex token " + NDJSONTokens.PITTokens.NAME + " missing.");
			if (!map.containsKey(NDJSONTokens.General.HGID)) throw new IOException("Vertex token " + NDJSONTokens.General.HGID + " missing.");
			if (!map.containsKey(NDJSONTokens.PITTokens.TYPE)) throw new IOException("Vertex token " + NDJSONTokens.PITTokens.TYPE + " missing.");
			if (!map.containsKey(NDJSONTokens.General.LAYER)) throw new IOException("Vertex token " + NDJSONTokens.General.LAYER + " missing.");
			
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