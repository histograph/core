package org.waag.histograph.queue;

public final class NDJSONTokens {
	
	public final class General {
		public static final String DATA = "data";
		public static final String SOURCE = "source";
		public static final String TYPE = "type";
		public static final String ACTION = "action";
	}
	
	public final class Types {
		public static final String VERTEX = "vertex";
		public static final String EDGE = "edge";
	}
	
	public final class Actions {
		public static final String ADD = "add";
		public static final String UPDATE = "update";
		public static final String DELETE = "delete";
	}
	
	public final class VertexTokens {
	    public static final String ID = "id";
	    public static final String TYPE = "type";
	    public static final String NAME = "name";		
	}
	
	public final class EdgeTokens {
		public static final String FROM = "from";
		public static final String TO = "to";
	    public static final String TYPE = "type";
	}
}