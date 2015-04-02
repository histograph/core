package org.waag.histograph.graph;

import org.neo4j.graphdb.Node;

public class AddedPITNotification extends Throwable {
	private static final long serialVersionUID = 836675179612943707L;

	private boolean hasURI;
	private Node node;
	private String uri;
	
	public AddedPITNotification (Node node) {
		super();
		this.node = node;
		hasURI = false;
	}
	
	public AddedPITNotification (Node node, String uri) {
		super();
		hasURI = true;
		this.node = node;
		this.uri = uri;
	}
	
	public String getURI () {
		if (hasURI) return uri;
		return null;
	}
	
	public Node getNode () {
		return node;
	}
	
	public boolean hasURI () {
		return hasURI;
	}
}