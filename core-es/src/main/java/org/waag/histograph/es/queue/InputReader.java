package org.waag.histograph.es.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class InputReader {
	
	public InputReader() {

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
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	// TODO
	private void updateVertex(JSONObject data, String layer) throws IOException {
		Map<String, String> params = getVertexParams(data, layer);
		
		System.out.println("Vertex updated.");
	}
	
	// TODO
	private void addVertex(JSONObject data, String layer) throws IOException {		
		Map<String, String> params = getVertexParams(data, layer);
		
		System.out.println("Vertex added.");
	}

	// TODO
	private void deleteVertex(JSONObject data, String layer) throws IOException {
		Map<String, String> params = getVertexParams(data, layer);
		
		System.out.println("Vertex deleted.");
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