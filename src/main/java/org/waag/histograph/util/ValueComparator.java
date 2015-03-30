package org.waag.histograph.util;

import java.util.Comparator;
import java.util.Map;

/**
 * A helper class to compare Map entries with integer values, for sorting on these values.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class ValueComparator implements Comparator<String> {

	Map<String, Integer> baseMap;
	
	/**
	 * Comparator for a TreeMap, sorts Nodes on the Integer value associated with it
	 * (i.e. the number of incoming/outgoing relationships of the node
	 * @param baseMap The map containing the tuples that are to be compared
	 */
	public ValueComparator(Map<String, Integer> baseMap) {
		this.baseMap = baseMap;
	}
	
	/**
	 * Compares two strings to each other, based on the integer values associated with them in the baseMap
	 */
	public int compare(String o1, String o2) {
		if (baseMap.get(o1) >= baseMap.get(o2)) {
			return -1;
		} else {
			return 1;
		}
	}
}