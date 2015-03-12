package org.waag.histograph.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.reasoner.ReasoningDefinitions;
import org.waag.histograph.reasoner.ReasoningDefinitions.RelationType;
import org.waag.histograph.util.HistographTokens;

public class GraphMethods {
	
	public static void addNode(GraphDatabaseService db, Map<String, String> params) throws IOException {		
		// Node lookup is omitted due to uniqueness constraint
		try (Transaction tx = db.beginTx()) {
			Node newPIT = db.createNode();
			newPIT.addLabel(ReasoningDefinitions.NodeType.PIT);

			// TODO This has resulted in a ConcurrentModificationException once...however, why? No concurrency here, nor key/value mutations while iterating!
			Iterator<Entry<String, String>> entries = params.entrySet().iterator();
			while (entries.hasNext()) {
				Entry<String, String> entry = entries.next();
				newPIT.setProperty(entry.getKey(), entry.getValue());
			}
			
			tx.success();
		}
	}
	
	public static void updateNode(GraphDatabaseService db, Map<String, String> params) throws IOException {
		// Verify node exists and get it
		Node node = GraphMethods.getNodeByHgid(db, params.get(HistographTokens.General.HGID));
		if (node == null) throw new IOException ("Node with " + HistographTokens.General.HGID + " '" + params.get(HistographTokens.General.HGID) + "' not found in graph.");
		
		// Update node
		try (Transaction tx = db.beginTx()) {
			// Remove all old properties
			for (String key : node.getPropertyKeys()) {
				node.removeProperty(key);
			}
			
			// Add all new properties
			for (Entry<String, String> entry : params.entrySet()) {
				node.setProperty(entry.getKey(), entry.getValue());
			}
			
			tx.success();
		}
		
		System.out.println("Node updated.");
	}
	
	public static void deleteNode(GraphDatabaseService db, Map<String, String> params) throws IOException {
		String hgid = params.get(HistographTokens.General.HGID);
		
		// Verify node exists and get it
		Node node = GraphMethods.getNodeByHgid(db, hgid);
		if (node == null) throw new IOException("Node with " + HistographTokens.General.HGID + " '" + hgid + "' not found in graph.");
		
		try (Transaction tx = db.beginTx()) {
			// Remove all associated relationships
			Iterable<Relationship> relationships = node.getRelationships();
			for (Relationship rel : relationships) {
				rel.delete();
			}
			
			// Remove node
			node.delete();
			tx.success();
		}
	}
	
	public static Node[] getNodesFromParams(GraphDatabaseService db, String hgidOrURI) throws IOException {
		// Search on URI first, return all found nodes in array
		Node[] nodes = GraphMethods.getNodesByURI(db, hgidOrURI);
		
		// If URI's are not found (i.e. null is returned), search for hgid
		if (nodes == null) {
			nodes = new Node[1];
			nodes[0] = GraphMethods.getNodeByHgid(db, hgidOrURI);
			if (nodes[0] == null) return null;
		}

		return nodes;
	}
	
	public static Relationship[] addRelation(GraphDatabaseService db, Map<String, String> params) throws IOException {
		// Search on URI first, return all found nodes in array
		Node[] fromNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.");
		if (toNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.");		
		
		RelationshipType relType = RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL));
		ArrayList<Relationship> relArray = new ArrayList<Relationship>();
		
		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				// Verify the absence of the new relation
				if (!GraphMethods.relationAbsent(db, fromNode, toNode, relType)) {
					String relationName = params.get(HistographTokens.RelationTokens.FROM) + " --" + relType.name() + "--> " + params.get(HistographTokens.RelationTokens.TO);
					System.out.println("Relation " + relationName + " already present in graph.");
					continue;
				}

				// Create relation between nodes
				try (Transaction tx = db.beginTx()) {
					Relationship rel = fromNode.createRelationshipTo(toNode, relType);
					rel.setProperty(HistographTokens.General.SOURCE, params.get(HistographTokens.General.SOURCE));
					relArray.add(rel);
					tx.success();
				}
			}
		}
		
		// Return the newly created relationships, if they are created
		if (relArray.size() > 0) {
			Relationship[] out = new Relationship[relArray.size()];
			out = relArray.toArray(out);
			return out;
		} else {
			return null;
		}
	}
	
	public static void updateRelation(GraphDatabaseService db, Map<String, String> params) throws IOException {
		throw new IOException("Updating relations not supported.");
	}
	
	public static void deleteRelation(GraphDatabaseService db, Map<String, String> params) throws IOException {
		// Search on URI first, return all found nodes in array
		Node[] fromNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.");
		if (toNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.");
		
		RelationshipType relType = RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL));
		
		// For all possible node pairs, find the relationship between them and delete when found
		boolean relationshipsRemoved = false;
		
		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				// Verify the existence of the relation
				Relationship r = GraphMethods.getRelation(db, fromNode, toNode, relType);
				if (r == null) {
					String relationName = params.get(HistographTokens.RelationTokens.FROM) + " --" + relType.name() + "--> " + params.get(HistographTokens.RelationTokens.TO);
					System.out.println("Relation " + relationName + " did not exist in graph.");
					continue;
				}
				relationshipsRemoved = true;

				// Remove relation between nodes
				try (Transaction tx = db.beginTx()) {
					r.delete();
					tx.success();
				}
			}
		}
		
		if (!relationshipsRemoved) {
			String relationName = params.get(HistographTokens.RelationTokens.FROM) + " --" + relType.name() + "--> " + params.get(HistographTokens.RelationTokens.TO);
			throw new IOException("No relationships '" + relationName + "' found in graph.");
		}
	}
	
	public static Node getNodeByHgid(GraphDatabaseService db, String hgid) throws IOException {
        try (Transaction ignored = db.beginTx()) {
        	try (ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, HistographTokens.General.HGID, hgid).iterator()) {
        		if (!i.hasNext()) return null;
        		Node out = i.next();
        		if (i.hasNext()) throw new IOException ("Multiple nodes with " + HistographTokens.General.HGID + " '" + hgid + "' found in graph.");
        		return out;
        	}
        }
	}	
	
	public static boolean nodeExistsByHgid(GraphDatabaseService db, String hgID) throws IOException {
		return (getNodeByHgid(db, hgID) != null);
	}
	
	public static boolean nodeAbsentByHgid(GraphDatabaseService db, String hgID) throws IOException {
		return (getNodeByHgid(db, hgID) == null);
	}
	
	public static Node[] getNodesByURI(GraphDatabaseService db, String uri) throws IOException {
		try (Transaction ignored = db.beginTx()) {
			try (ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, HistographTokens.PITTokens.URI, uri).iterator()) {
				if (!i.hasNext()) return null;
				ArrayList<Node> list = new ArrayList<Node>();
				while (i.hasNext()) {
					list.add(i.next());
				}
				Node[] out = new Node[list.size()];
				out = list.toArray(out);
				return out;
			}
		}
	}
	
	public static Relationship getRelation(GraphDatabaseService db, Node fromNode, Node toNode, RelationshipType type) throws IOException {
		try (Transaction tx = db.beginTx()) {
			for (Relationship r : fromNode.getRelationships(type, Direction.OUTGOING)) {
				if (r.getEndNode().equals(toNode)) {
					return r;
				}
			}
		}
		return null;
	}
	
	public static boolean relationExists(GraphDatabaseService db, Node fromNode, Node toNode, RelationshipType type) throws IOException {
		return (getRelation(db, fromNode, toNode, type) != null);
	}
	
	public static boolean relationAbsent(GraphDatabaseService db, Node fromNode, Node toNode, RelationshipType type) throws IOException {
		return (getRelation(db, fromNode, toNode, type) == null);
	}
}