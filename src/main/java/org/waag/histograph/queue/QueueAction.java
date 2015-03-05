package org.waag.histograph.queue;

import java.util.Map;

public class QueueAction {

	private ActionHandler handler;
	private String type;
	private String action;
	private Map<String, String> params;
	
	public QueueAction (ActionHandler handler, String type, String action, Map<String, String> params) {
		this.handler = handler;
		this.type = type;
		this.action = action;
		this.params = params;
	}
	
	public ActionHandler getHandler () {
		return handler;
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
