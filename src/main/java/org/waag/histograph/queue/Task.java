package org.waag.histograph.queue;

import java.util.Map;

public class Task {

	private String type;
	private String action;
	private Map<String, String> params;
	
	public Task (String type, String action, Map<String, String> params) {
		this.type = type;
		this.action = action;
		this.params = params;
	}

	public String getType () {
		return type;
	}
	
	public String getAction () {
		return action;
	}
	
	public Map<String, String> getParams () {
		return params;
	}
}