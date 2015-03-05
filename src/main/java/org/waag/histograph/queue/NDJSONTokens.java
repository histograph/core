package org.waag.histograph.queue;

public final class NDJSONTokens {
	
	public final class General {
		public static final String DATA = "data";
		public static final String LAYER = "layer";
		public static final String TYPE = "type";
		public static final String ACTION = "action";
	    public static final String HGID = "hgid";
	    public static final String TARGET = "target";
	}
	
	public final class Types {
		public static final String PIT = "pit";
		public static final String RELATION = "relation";
	}
	
	public final class Actions {
		public static final String ADD = "add";
		public static final String UPDATE = "update";
		public static final String DELETE = "delete";
	}
	
	public final class Targets {
		public static final String GRAPH = "graph";
		public static final String ELASTICSEARCH = "es";
	}
	
	public final class PITTokens {
	    public static final String ID = "id";
	    public static final String TYPE = "type";
	    public static final String NAME = "name";
	    public static final String URI = "uri";
	    public static final String GEOMETRY = "geometry";
	    public static final String STARTDATE = "startDate";
	    public static final String ENDDATE = "endDate";
	    public static final String DATA = "data";
	}
	
	public final class RelationTokens {
		public static final String FROM = "from";
		public static final String TO = "to";
	    public static final String LABEL = "label";
	}
}