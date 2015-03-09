package org.waag.histograph.server;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<String> {

	Map<String, Integer> baseMap;
	
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