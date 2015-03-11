package org.waag.histograph.traversal;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<String> {

	Map<String, Integer> baseMap;
	
	// Comparator for a TreeMap, sorts Nodes on the Integer value associated with it
	// (i.e. the number of incoming/outgoing relationships of the node
	public ValueComparator(Map<String, Integer> baseMap) {
		this.baseMap = baseMap;
	}
	
	public int compare(String o1, String o2) {
		if (baseMap.get(o1) >= baseMap.get(o2)) {
			return -1;
		} else {
			return 1;
		}
	}
}