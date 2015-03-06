package org.waag.histograph.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.util.HistographTokens;

public class InputReader {
	
	public static QueueTask parse(JSONObject obj) throws IOException {
		String layer;
		String target;
		
		try {
			switch (obj.get(HistographTokens.General.TARGET).toString()) {
			case HistographTokens.Targets.GRAPH:
				target = HistographTokens.Targets.GRAPH;
				break;
			case HistographTokens.Targets.ELASTICSEARCH:
				target = HistographTokens.Targets.ELASTICSEARCH;
				break;
			default:
				throw new IOException("Invalid target received: " + obj.get(HistographTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			// If the TARGET key is not found, send task to both
			target = HistographTokens.Targets.BOTH;
		}
		
		try {
			layer = obj.get(HistographTokens.General.LAYER).toString();
			switch (obj.get(HistographTokens.General.ACTION).toString()) {
			case HistographTokens.Actions.ADD:
				return parseAdd(obj, layer, target);
			case HistographTokens.Actions.DELETE:
				return parseDelete(obj, layer, target);
			case HistographTokens.Actions.UPDATE:
				return parseUpdate(obj, layer, target);
			default:
				throw new IOException("Invalid action received: " + obj.get(HistographTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No layer or action in JSON input.");
		}	
	}
	
	private static QueueTask parseAdd(JSONObject obj, String layer, String target) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(HistographTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(HistographTokens.General.TYPE).toString()) {
			case HistographTokens.Types.PIT:
				return addPIT(data, layer, target);
			case HistographTokens.Types.RELATION:
				return addRelation(data, layer, target);
			default:
				throw new IOException("Invalid type received: " + obj.get(HistographTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static QueueTask parseDelete(JSONObject obj, String layer, String target) throws IOException {		
		JSONObject data;
		try {
			data = obj.getJSONObject(HistographTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(HistographTokens.General.TYPE).toString()) {
			case HistographTokens.Types.PIT:
				return deletePIT(data, layer, target);
			case HistographTokens.Types.RELATION:
				return deleteRelation(data, layer, target);
			default:
				throw new IOException("Invalid type received: " + obj.get(HistographTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static QueueTask parseUpdate(JSONObject obj, String layer, String target) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(HistographTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(HistographTokens.General.TYPE).toString()) {
			case HistographTokens.Types.PIT:
				return updatePIT(data, layer, target);
			case HistographTokens.Types.RELATION:
				return updateRelation(data, layer, target);
			default:
				throw new IOException("Invalid type received: " + obj.get(HistographTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static QueueTask addPIT(JSONObject data, String layer, String target) throws IOException {		
		Map<String, String> params = getPITParams(data, layer);		
		return new QueueTask(target, HistographTokens.Types.PIT, HistographTokens.Actions.ADD, params);
	}
	
	private static QueueTask updatePIT(JSONObject data, String layer, String target) throws IOException {
		Map<String, String> params = getPITParams(data, layer);
		return new QueueTask(target, HistographTokens.Types.PIT, HistographTokens.Actions.UPDATE, params);
	}
	
	private static QueueTask deletePIT(JSONObject data, String layer, String target) throws IOException {
		try {
			String hgid = parseHGid(layer, data.get(HistographTokens.PITTokens.ID).toString());
			Map<String, String> params = new HashMap<String, String>();
			params.put(HistographTokens.General.HGID, hgid);
			return new QueueTask(target, HistographTokens.Types.PIT, HistographTokens.Actions.DELETE, params);
		} catch (JSONException e) {
			throw new IOException("Cannot delete PIT: " + HistographTokens.General.HGID + " missing.");
		}
	}
	
	private static QueueTask addRelation(JSONObject data, String layer, String target) throws IOException {
		Map<String, String> params = getRelationParams(data, layer);
		return new QueueTask(target, HistographTokens.Types.RELATION, HistographTokens.Actions.ADD, params);
	}
	
	private static QueueTask updateRelation(JSONObject data, String layer, String target) throws IOException {
		throw new IOException("Updating relations not supported.");
	}
	
	private static QueueTask deleteRelation(JSONObject data, String layer, String target) throws IOException {
		Map<String, String> params = getRelationParams(data, layer);
		return new QueueTask(target, HistographTokens.Types.RELATION, HistographTokens.Actions.DELETE, params);
	}
	
	private static Map<String, String> getPITParams(JSONObject data, String layer) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			map.put(HistographTokens.PITTokens.NAME, data.get(HistographTokens.PITTokens.NAME).toString());
			map.put(HistographTokens.General.HGID, parseHGid(layer, data.get(HistographTokens.PITTokens.ID).toString()));
			map.put(HistographTokens.PITTokens.TYPE, data.get(HistographTokens.PITTokens.TYPE).toString());
			map.put(HistographTokens.General.LAYER, layer);
			
			// Optional predefined tokens
			if (data.has(HistographTokens.PITTokens.GEOMETRY)) {
				map.put(HistographTokens.PITTokens.GEOMETRY, data.get(HistographTokens.PITTokens.GEOMETRY).toString());
			}
			if (data.has(HistographTokens.PITTokens.STARTDATE)) {
				map.put(HistographTokens.PITTokens.STARTDATE, data.get(HistographTokens.PITTokens.STARTDATE).toString());
			}
			if (data.has(HistographTokens.PITTokens.ENDDATE)) {
				map.put(HistographTokens.PITTokens.ENDDATE, data.get(HistographTokens.PITTokens.ENDDATE).toString());
			}
			if (data.has(HistographTokens.PITTokens.URI)) {
				map.put(HistographTokens.PITTokens.URI, data.get(HistographTokens.PITTokens.URI).toString());
			}
			
			// Other specific data, add as single string
			if (data.has(HistographTokens.PITTokens.DATA)) {
				map.put(HistographTokens.PITTokens.DATA, data.getJSONObject(HistographTokens.PITTokens.DATA).toString());
			}
			
			// Remove keys with empty values
			Iterator<Entry<String, String>> iter = map.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, String> entry = iter.next();
				if (entry.getValue().equals("")) iter.remove();
			}
			
			// Validate existence of compulsory keys
			if (!map.containsKey(HistographTokens.PITTokens.NAME)) throw new IOException("PIT token " + HistographTokens.PITTokens.NAME + " missing.");
			if (!map.containsKey(HistographTokens.General.HGID)) throw new IOException("PIT token " + HistographTokens.General.HGID + " missing.");
			if (!map.containsKey(HistographTokens.PITTokens.TYPE)) throw new IOException("PIT token " + HistographTokens.PITTokens.TYPE + " missing.");
			if (!map.containsKey(HistographTokens.General.LAYER)) throw new IOException("PIT token " + HistographTokens.General.LAYER + " missing.");
			
			return map;
		} catch (JSONException e) {
			System.out.println("Error: " + e.getMessage());
			throw new IOException("PIT token(s) missing (" + HistographTokens.PITTokens.ID + "/" + HistographTokens.PITTokens.NAME + "/" + HistographTokens.PITTokens.TYPE + ").");
		}
	}
	
	private static Map<String, String> getRelationParams(JSONObject data, String layer) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			map.put(HistographTokens.RelationTokens.FROM, parseHGid(layer, data.get(HistographTokens.RelationTokens.FROM).toString()));
			map.put(HistographTokens.RelationTokens.TO, parseHGid(layer, data.get(HistographTokens.RelationTokens.TO).toString()));
			map.put(HistographTokens.RelationTokens.LABEL, data.get(HistographTokens.RelationTokens.LABEL).toString());
			map.put(HistographTokens.General.LAYER, layer);			
			return map;
		} catch (JSONException e) {
			throw new IOException("Relation token(s) missing (" + HistographTokens.RelationTokens.FROM + "/" + HistographTokens.RelationTokens.TO + "/" + HistographTokens.RelationTokens.LABEL + ").");
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