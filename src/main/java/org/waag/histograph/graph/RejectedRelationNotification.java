package org.waag.histograph.graph;

import java.util.Map;

public class RejectedRelationNotification extends Throwable {

	private static final long serialVersionUID = -2660083407877043518L;
	
	private Map<String, String>[] relMaps;
	
	public RejectedRelationNotification (String message, Map<String, String>[] relMaps) {
		super(message);
		this.relMaps = relMaps;
	}
	
	@SuppressWarnings("unchecked")
	public RejectedRelationNotification (String message, Map<String, String> relMap) {
		super(message);
		Map<String, String>[] relMaps = new Map[1];
		relMaps[0] = relMap;
		this.relMaps = relMaps;
	}
	
	public RejectedRelationNotification (Map<String, String>[] relMaps) {
		this.relMaps = relMaps;
	}
	
	@SuppressWarnings("unchecked")
	public RejectedRelationNotification (Map<String, String> relMap) {
		Map<String, String>[] relMaps = new Map[1];
		relMaps[0] = relMap;
		this.relMaps = relMaps;
	}
	
	public Map<String, String>[] getRelationParams () {
		return relMaps;
	}
}