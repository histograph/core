package org.waag.histograph.reasoner;

import java.util.ArrayList;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * A class containing reasoning rules and definitions for the Neo4j graph.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public final class ReasoningDefinitions {
	
	/**
	 * The single node type used for reasoning in the Neo4j graph is 'PIT'.
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public enum NodeType implements Label {
		PIT
	}
	
	/**
	 * An enumeration of all Relationship types (defined at http://histograph.io/concepts/) and their String representations.
	 * @author Rutger van Willigen
	 * @author Bert Spaan
	 */
	public enum RelationType implements RelationshipType {
		SAMEAS("owl:sameAs"),
		ABSORBED("hg:absorbed"),
		ABSORBEDBY("hg:absorbedBy"),
		CONTAINS("hg:contains"),
		HASGEOFEATURE("hg:hasGeoFeature"),
		HASNAME("hg:hasName"),
		HASPITTYPE("hg:hasPitType"),
		HASPROVENTITY("hg:hasProvEntity"),
		HASTIMETEMPORALENTITY("hg:hasTimeTemporalEntity"),
		ISUSEDFOR("hg:isUsedFor"),
		SAMEHGCONCEPT("hg:sameHgConcept"),
		LIESIN("hg:liesIn");
		
		private final String label;
		
		private RelationType (String label) {
			this.label = label;
		}
		
		/**
		 * Get the String representation of a relationship.
		 * @return The String representation of the relationship.
		 */
		public String toString () {
			return this.label;
		}
		
		/**
		 * Get the RelationType object associated with a String representation.
		 * @param label The string representation of the relationship.
		 * @return The RelationType object, or null if no object with this String representation is found.
		 */
		public static RelationType fromLabel(String label) {
			if (label != null) {
				for (RelationType b : RelationType.values()) {
					if (label.equals(b.toString())) {
						return b;
					}
				}
			}
			return null;
		}
		
		/**
		 * Get the RelationType object associated with a (more general) RelationshipType object.
		 * @param type The RelationshipType object. Typically returned from the {@link org.neo4j.graphdb.Relationship#getType()} method.
		 * @return The RelationType associated with the provided RelationshipType, or null if no such RelationType was found.
		 */
		public static RelationType fromRelationshipType(RelationshipType type) {
			for (RelationType r : RelationType.values()) {
				if (r.name().equals(type.name())) return r;
			}
			return null;
		}
	}
	
	/**
	 * Contains the String representations of all primary relations defined at http://histograph.io/concepts/.
	 */
	public final static String[] PRIMARY_RELATIONS = {		RelationType.SAMEHGCONCEPT.toString(),
															RelationType.ABSORBEDBY.toString(),
															RelationType.ISUSEDFOR.toString(),
															RelationType.LIESIN.toString()
													};
	
	/**
	 * Contains all transitive relations, i.e. (a) --[rel]--) (b) --[rel]--) (c) IMPL (a) --[rel]--) (c)
	 */
	public final static String[] TRANSITIVE_RELATIONS = {	RelationType.LIESIN.toString()
													};
	
	/**
	 * Contains all atomic inferences as defined in http://histograph.github.io/
	 */
	private final static class AtomicInferences {
		private final static String[] ATOMIC_INF_SAMEAS = {		RelationType.SAMEHGCONCEPT.toString()
													};
	}
	
	
	/**
	 * Get all atomic relationships associated with a provided relationship.
	 * @param label The primary relationship's String representation.
	 * @return An array of atomic relationships associated with the primary relationship, or null if no atomic relationships are associated.
	 */
	public static String[] getAtomicRelationsFromLabel(String label) {
		switch (RelationType.fromLabel(label)) {
		case SAMEAS:
			return ReasoningDefinitions.AtomicInferences.ATOMIC_INF_SAMEAS;
		default:
			return null;
		}
	}
	
	/**
	 * Returns an array of all defined relationships.
	 * @return An array containing String representations of all defined relationships.
	 */
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
	
	/**
	 * Returns all primary relationships associated with a given atomic relationship.
	 * @param relation An atomic relationship.
	 * @return An array of String representations of the primary relationships associated with the given atomic relationship,
	 * or null if no such relationships are found.
	 */
	public static String[] getPrimaryRelationsFromAtomic(String relation) {
		ArrayList<String> labels = new ArrayList<String>();
		for (String primary : PRIMARY_RELATIONS) {
			for (String atomic : getAtomicRelationsFromLabel(primary)) {
				if (relation.equals(atomic)) labels.add(primary);
			}
		}
		if (labels.size() == 0) return null;
		String[] out = new String[labels.size()];
		out = labels.toArray(out);
		return out;
	}
}