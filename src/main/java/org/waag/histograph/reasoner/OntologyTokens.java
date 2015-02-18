package org.waag.histograph.reasoner;

import java.util.ArrayList;

public final class OntologyTokens {
	
	public static final String SAMEAS = "hg:sameAs";
	public static final String ABSORBEDBY = "hg:absorbedBy";
	public static final String ISUSEDFOR = "hg:isUsedFor";
	public static final String LIESIN = "hg:liesIn";
	
	public final static String[] PRIMARY_RELATIONS = {SAMEAS, ABSORBEDBY, ISUSEDFOR, LIESIN};
	
	private final static class AtomicInferences {
		private final static String[] ATOMIC_SAMEAS = {"hga:conceptIdentical", "hga:typeIdentical"};
		private final static String[] ATOMIC_ABSORBEDBY = {"hga:conceptIn", "hga:periodBefore", "hga:typeIdentical", "hga:geometryIntersects"};
		private final static String[] ATOMIC_ISUSEDFOR = {"hga:conceptIdentical", "hga:typeIdentical"};
		private final static String[] ATOMIC_LIESIN = {"hga:geometryIntersects"};
	}
	
	public static String[] getAtomicRelationsFromLabel(String label) {
		switch (label) {
		case OntologyTokens.SAMEAS:
			return OntologyTokens.AtomicInferences.ATOMIC_SAMEAS;
		case OntologyTokens.ABSORBEDBY:
			return OntologyTokens.AtomicInferences.ATOMIC_ABSORBEDBY;
		case OntologyTokens.ISUSEDFOR:
			return OntologyTokens.AtomicInferences.ATOMIC_ISUSEDFOR;
		case OntologyTokens.LIESIN:
			return OntologyTokens.AtomicInferences.ATOMIC_LIESIN;
		default:
			return null;
		}
	}
	
	public static String[] getAllRelations () {
		ArrayList<String> relations = new ArrayList<String>();
		for (String s : PRIMARY_RELATIONS) {
			relations.add(s);
			String[] atomics = getAtomicRelationsFromLabel(s);
			for (String a : atomics) {
				if (!relations.contains(a)) {
					relations.add(a);
				}
			}
		}
		String[] out = new String[relations.size()];
		out = relations.toArray(out);
		return out;
	}
	
	public static String[] getPrimaryRelationsFromAtomic(String relation) {
		ArrayList<String> labels = new ArrayList<String>();
		for (String primary : PRIMARY_RELATIONS) {
			for (String atomic : getAtomicRelationsFromLabel(primary)) {
				if (relation.equals(atomic)) labels.add(primary);
			}
		}
		String[] out = new String[labels.size()];
		out = labels.toArray(out);
		return out;
	}
	
}