package org.waag.histograph.queue;

import java.util.Map;

public class QueueTask {

	private String target;
	private String type;
	private String action;
	private Map<String, String> params;
	
	public QueueTask (String target, String type, String action, Map<String, String> params) {
		this.target = target;
		this.type = type;
		this.action = action;
		this.params = params;
	}
	
	public String getTarget () {
		return target;
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
