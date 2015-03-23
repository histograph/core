package org.waag.histograph.util;

/**
 * A class in which all Histograph tokens are defined. These tokens are fixed
 * throughout all Histograph classes and should be modified here only.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public final class HistographTokens {
	
	/**
	 * General tokens applicable for all object types.
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public final class General {
		public static final String DATA = "data";
		public static final String SOURCE = "source";
		public static final String TYPE = "type";
		public static final String ACTION = "action";
	    public static final String HGID = "hgid";
	    public static final String TARGET = "target";
	}
	
	/**
	 * Object types
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public final class Types {
		public static final String PIT = "pit";
		public static final String RELATION = "relation";
	}
	
	/**
	 * Action types
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public final class Actions {
		public static final String ADD = "add";
		public static final String UPDATE = "update";
		public static final String DELETE = "delete";
	}
	
	/**
	 * Target threads for tasks to be performed
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public final class Targets {
		public static final String GRAPH = "graph";
		public static final String ELASTICSEARCH = "es";
		public static final String BOTH = "both";
	}
	
	/**
	 * Tokens applicable for PITs
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public final class PITTokens {
	    public static final String ID = "id";
	    public static final String TYPE = "type";
	    public static final String NAME = "name";
	    public static final String URI = "uri";
	    public static final String GEOMETRY = "geometry";
	    public static final String HASBEGINNING = "hasBeginning";
	    public static final String HASEND = "hasEnd";
	    public static final String DATA = "data";
	}
	
	/**
	 * Tokens applicable for relations
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public final class RelationTokens {
		public static final String FROM = "from";
		public static final String TO = "to";
	    public static final String LABEL = "label";
	}
}