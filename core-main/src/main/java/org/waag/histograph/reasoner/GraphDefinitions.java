package org.waag.histograph.reasoner;

import java.util.ArrayList;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public final class GraphDefinitions {
	
	public enum NodeType implements Label {
		PIT;
	}
	
	public enum RelationType implements RelationshipType {
		SAMEAS("hg:sameAs"),
		ABSORBEDBY("hg:absorbedBy"),
		ISUSEDFOR("hg:isUsedFor"),
		LIESIN("hg:liesIn"),
		CONCEPTIDENTICAL("hga:conceptIdentical"),
		TYPEIDENTICAL("hga:typeIdentical"),
		CONCEPTIN("hga:conceptIn"),
		PERIODBEFORE("hga:periodBefore"),
		GEOMETRYINTERSECTS("hga:geometryIntersects");
		
		private final String label;
		
		private RelationType (String label) {
			this.label = label;
		}
	
		public String getLabel () {
			return this.label;
		}
		
		public static RelationType fromLabel(String label) {
			if (label != null) {
				for (RelationType b : RelationType.values()) {
					if (label.equals(b.getLabel())) {
						return b;
					}
				}
			}
			return null;
		}
	}
	
	// All primary relations as defined in http://histograph.github.io/
	public final static String[] PRIMARY_RELATIONS = {		RelationType.SAMEAS.getLabel(),
															RelationType.ABSORBEDBY.getLabel(),
															RelationType.ISUSEDFOR.getLabel(),
															RelationType.LIESIN.getLabel()
													};
	
	// All transitive relations, i.e. (a) --[rel]--> (b) --[rel]--> (c) IMPL (a) --[rel]--> (c)
	public final static String[] TRANSITIVE_RELATIONS = {	RelationType.LIESIN.getLabel(), 
															RelationType.PERIODBEFORE.getLabel(), 
															RelationType.CONCEPTIN.getLabel()
													};

	// All atomic inferences as defined in http://histograph.github.io/
	private final static class AtomicInferences {
		private final static String[] ATOMIC_SAMEAS = {		RelationType.CONCEPTIDENTICAL.getLabel(), 
															RelationType.TYPEIDENTICAL.getLabel()
													};
		
		private final static String[] ATOMIC_ABSORBEDBY = {	RelationType.CONCEPTIN.getLabel(), 
															RelationType.PERIODBEFORE.getLabel(),
															RelationType.TYPEIDENTICAL.getLabel(),
															RelationType.GEOMETRYINTERSECTS.getLabel()
													};
		
		private final static String[] ATOMIC_ISUSEDFOR = {	RelationType.CONCEPTIDENTICAL.getLabel(), 
															RelationType.TYPEIDENTICAL.getLabel()
													};
		
		private final static String[] ATOMIC_LIESIN = {		RelationType.GEOMETRYINTERSECTS.getLabel()
													};
	}
	
	public static String[] getAtomicRelationsFromLabel(String label) {
		switch (RelationType.fromLabel(label)) {
		case SAMEAS:
			return GraphDefinitions.AtomicInferences.ATOMIC_SAMEAS;
		case ABSORBEDBY:
			return GraphDefinitions.AtomicInferences.ATOMIC_ABSORBEDBY;
		case ISUSEDFOR:
			return GraphDefinitions.AtomicInferences.ATOMIC_ISUSEDFOR;
		case LIESIN:
			return GraphDefinitions.AtomicInferences.ATOMIC_LIESIN;
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