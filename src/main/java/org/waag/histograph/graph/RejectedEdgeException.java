package org.waag.histograph.graph;

import java.util.Map;

public class RejectedEdgeException extends Exception {

	private static final long serialVersionUID = -2660083407877043518L;
	
	private String nodeHgidOrURI;
	private Map<String, String> params;
	
	public RejectedEdgeException (String message, String nodeHgidOrURI, Map<String, String> params) {
		super(message);
		this.nodeHgidOrURI = nodeHgidOrURI;
		this.params = params;
	}
	
	public String getNodeHgidOrURI () {
		return nodeHgidOrURI;
	}
	
	public Map<String, String> getParams () {
		return params;
	}
}