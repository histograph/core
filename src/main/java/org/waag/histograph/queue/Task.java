package org.waag.histograph.queue;

import java.util.Map;

import org.waag.histograph.util.InputReader;

/**
 * A class in which a single graph / Elasticsearch task is put. Each task consists of three
 * components -- an object type that is to be processed (PIT or Relationship), an action to
 * be performed (either add, update or remove) and a map containing all PIT / Relationship
 * parameters. Task objects are typically created by the {@link InputReader} class.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class Task {

	private String type;
	private String action;
	private Map<String, String> params;
	
	/**
	 * Constructor in which all parameters are set.
	 * @param type Object type that is to be processed. Should be one of {@link org.waag.histograph.util.HistographTokens.Types}.
	 * @param action Action that is to be performed. Should be one of {@link org.waag.histograph.util.HistographTokens.Actions}.
	 * @param params Map containing the object's parameters. Typically created by the {@link InputReader} class.
	 */
	public Task (String type, String action, Map<String, String> params) {
		this.type = type;
		this.action = action;
		this.params = params;
	}

	/**
	 * Returns the object type of this task.
	 * @return The object type of this task.
	 */
	public String getType () {
		return type;
	}
	
	/**
	 * Returns the action to be performed in this task.
	 * @return The action to be performed in this task.
	 */
	public String getAction () {
		return action;
	}
	
	/**
	 * Returns a map of the parameters of the object in this task.
	 * @return A map of the parameters of the object in this task.
	 */
	public Map<String, String> getParams () {
		return params;
	}
}