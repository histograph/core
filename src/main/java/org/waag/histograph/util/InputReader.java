package org.waag.histograph.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.queue.Task;
import org.waag.histograph.util.HistographTokens.PITIdentifyingMethod;

/**
 * A class that parses JSON tasks typically found in the Redis queue and transforms them to {@link Task} objects.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class InputReader {
	
	/**
	 * Parses a JSONObject (typically found in the Redis queue) to a {@link Task} object.
	 * @param obj The JSON representation of the task to be carried out.
	 * @return A task object in which all parameters are stored.
	 * @throws IOException Thrown when a parsing error is encountered.
	 */
	public static Task parse(JSONObject obj) throws IOException {		
		try {
			String source = obj.get(HistographTokens.General.SOURCE).toString();
			switch (obj.get(HistographTokens.General.ACTION).toString()) {
			case HistographTokens.Actions.ADD:
				return parseAdd(obj, source);
			case HistographTokens.Actions.DELETE:
				return parseDelete(obj, source);
			case HistographTokens.Actions.UPDATE:
				return parseUpdate(obj, source);
			case HistographTokens.Actions.ADD_TO_REJECTED:
				return parseAddRejected(obj, source);
			default:
				throw new IOException("Invalid action received: " + obj.get(HistographTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No source or action in JSON input.");
		}	
	}
	
	private static Task parseAdd(JSONObject obj, String source) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(HistographTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(HistographTokens.General.TYPE).toString()) {
			case HistographTokens.Types.PIT:
				return addPIT(data, source);
			case HistographTokens.Types.RELATION:
				return addRelation(data, source);
			default:
				throw new IOException("Invalid type received: " + obj.get(HistographTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static Task parseDelete(JSONObject obj, String source) throws IOException {		
		JSONObject data;
		try {
			data = obj.getJSONObject(HistographTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(HistographTokens.General.TYPE).toString()) {
			case HistographTokens.Types.PIT:
				return deletePIT(data, source);
			case HistographTokens.Types.RELATION:
				return deleteRelation(data, source);
			default:
				throw new IOException("Invalid type received: " + obj.get(HistographTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static Task parseUpdate(JSONObject obj, String source) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(HistographTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(HistographTokens.General.TYPE).toString()) {
			case HistographTokens.Types.PIT:
				return updatePIT(data, source);
			case HistographTokens.Types.RELATION:
				return updateRelation(data, source);
			default:
				throw new IOException("Invalid type received: " + obj.get(HistographTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	private static Task parseAddRejected(JSONObject obj, String source) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(HistographTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		String type = null;
		try {
			type = obj.get(HistographTokens.General.TYPE).toString();
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
		
		switch (type) {
		case HistographTokens.Types.RELATION:
			try {
				return addRejectedRelation(data, source);
			} catch (JSONException e) {
				throw new IOException("No rejection cause in JSON input.");
			}
		default:
			throw new IOException("Invalid rejected type received: " + obj.get(HistographTokens.General.TYPE).toString());
		}
		
	}
	
	private static Task addPIT(JSONObject data, String source) throws IOException {		
		Map<String, String> params = getPITParams(data, source);		
		return new Task(HistographTokens.Types.PIT, HistographTokens.Actions.ADD, params);
	}
	
	private static Task updatePIT(JSONObject data, String source) throws IOException {
		Map<String, String> params = getPITParams(data, source);
		return new Task(HistographTokens.Types.PIT, HistographTokens.Actions.UPDATE, params);
	}
	
	private static Task deletePIT(JSONObject data, String source) throws IOException {
		try {
			String hgid = parseHgid(source, data.get(HistographTokens.PITTokens.ID).toString());
			Map<String, String> params = new HashMap<String, String>();
			params.put(HistographTokens.General.HGID, hgid);
			return new Task(HistographTokens.Types.PIT, HistographTokens.Actions.DELETE, params);
		} catch (JSONException e) {
			throw new IOException("Cannot delete PIT: " + HistographTokens.General.HGID + " missing.");
		}
	}
	
	private static Task addRelation(JSONObject data, String source) throws IOException {
		Map<String, String> params = getRelationParams(data, source);
		return new Task(HistographTokens.Types.RELATION, HistographTokens.Actions.ADD, params);
	}
	
	private static Task updateRelation(JSONObject data, String source) throws IOException {
		throw new IOException("Updating relations not supported.");
	}
	
	private static Task deleteRelation(JSONObject data, String source) throws IOException {
		Map<String, String> params = getRelationParams(data, source);
		return new Task(HistographTokens.Types.RELATION, HistographTokens.Actions.DELETE, params);
	}
	
	private static Task addRejectedRelation(JSONObject data, String source) throws IOException {
		Map<String, String> params = getRelationParams(data, source);
		return new Task(HistographTokens.Types.RELATION, HistographTokens.Actions.ADD_TO_REJECTED, params);
	}
	
	private static Map<String, String> getPITParams(JSONObject data, String source) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			map.put(HistographTokens.PITTokens.NAME, data.get(HistographTokens.PITTokens.NAME).toString());
			map.put(HistographTokens.General.HGID, parseHgid(source, data.get(HistographTokens.PITTokens.ID).toString()));
			map.put(HistographTokens.PITTokens.TYPE, data.get(HistographTokens.PITTokens.TYPE).toString());
			map.put(HistographTokens.General.SOURCE, source);
			
			// Optional predefined tokens
			if (data.has(HistographTokens.PITTokens.GEOMETRY)) {
				map.put(HistographTokens.PITTokens.GEOMETRY, data.get(HistographTokens.PITTokens.GEOMETRY).toString());
			}
			if (data.has(HistographTokens.PITTokens.HASBEGINNING)) {
				map.put(HistographTokens.PITTokens.HASBEGINNING, data.get(HistographTokens.PITTokens.HASBEGINNING).toString());
			}
			if (data.has(HistographTokens.PITTokens.HASEND)) {
				map.put(HistographTokens.PITTokens.HASEND, data.get(HistographTokens.PITTokens.HASEND).toString());
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
			if (!map.containsKey(HistographTokens.General.SOURCE)) throw new IOException("PIT token " + HistographTokens.General.SOURCE + " missing.");
			
			return map;
		} catch (JSONException e) {
			System.out.println("Error: " + e.getMessage());
			throw new IOException("PIT token(s) missing (" + HistographTokens.PITTokens.ID + "/" + HistographTokens.PITTokens.NAME + "/" + HistographTokens.PITTokens.TYPE + ").");
		}
	}
	
	private static Map<String, String> getRelationParams(JSONObject data, String source) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			String from = parseHgid(source, data.get(HistographTokens.RelationTokens.FROM).toString());
			String to = parseHgid(source, data.get(HistographTokens.RelationTokens.TO).toString());

			PITIdentifyingMethod fromIdMethod = getPITIdentifierMethod(from);
			PITIdentifyingMethod toIdMethod = getPITIdentifierMethod(to);
			
			String label = data.get(HistographTokens.RelationTokens.LABEL).toString();
			
			map.put(HistographTokens.RelationTokens.FROM, from);
			map.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
			map.put(HistographTokens.RelationTokens.TO, to);
			map.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
			map.put(HistographTokens.RelationTokens.LABEL, label);
			map.put(HistographTokens.General.SOURCE, source);
			
			if (data.has(HistographTokens.RelationTokens.REJECTION_CAUSE)) {
				String cause = data.get(HistographTokens.RelationTokens.REJECTION_CAUSE).toString();
				PITIdentifyingMethod causeIdMethod = getPITIdentifierMethod(cause);

				map.put(HistographTokens.RelationTokens.REJECTION_CAUSE, cause);				
				map.put(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD, causeIdMethod.toString());
			}
			
			return map;
		} catch (JSONException e) {
			throw new IOException("Relation token(s) missing (" + HistographTokens.RelationTokens.FROM + "/" + HistographTokens.RelationTokens.TO + "/" + HistographTokens.RelationTokens.LABEL + ").");
		}
	}
	
	private static PITIdentifyingMethod getPITIdentifierMethod (String identifier) {
		if (isURI(identifier)) {
			return PITIdentifyingMethod.URI;
		} else {
			return PITIdentifyingMethod.HGID;
		}
	}
	
	private static boolean isURI (String string) {
		return (string.startsWith("http://") || string.startsWith("https://"));
	}
	
	private static String parseHgid (String source, String id) {
		// We assume URI's and hgids always contain at least one forward slash because of the hgid structure, e.g. 'geonames/1283416'
		CharSequence delimiter = "/";
		if (!id.contains(delimiter)) {
			return source + delimiter + id;
		} else {
			return id;
		}
	}
}
