package org.waag.histograph.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.ConstraintViolationException;
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

/**
 * Class containing several static methods for basic Neo4j graph operations. All methods
 * require an added {@link GraphDatabaseService} object that encapsulates the Neo4j graph,
 * on which the operations are to be performed.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class GraphMethods {
	
	/**
	 * Adds a node to the graph. Nodes have a uniqueness constraint on the 'hgid' property -- if a node with 
	 * the same hgid was already present, the node is not added and a ConstraintViolationException is thrown.
	 * @param db The Neo4j graph object 
	 * @param params A map containing all parameters to be added to the node. Commonly created by 
	 * the {@link org.waag.histograph.util.InputReader} class, returned with {@link org.waag.histograph.queue.Task#getParams()}.
	 * @throws ConstraintViolationException Thrown if a node with the same hgid was already present.
	 */
	public static void addNode(GraphDatabaseService db, Map<String, String> params) throws ConstraintViolationException {		
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
	
	/**
	 * Updates an existing node in the graph, based on the hgid, which remains the same. All other properties
	 * are removed and replaced by the parameters in the params object.
	 * @param db The Neo4j graph object
	 * @param params A map containing the hgid and all parameters to be added to the node. Commonly created by 
	 * the {@link org.waag.histograph.util.InputReader} class, returned with {@link org.waag.histograph.queue.Task#getParams()}.
	 * @throws IOException Thrown if no node with this hgid was present in the graph.
	 */
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
	}
	
	/**
	 * Removes a node and all its associated relationships from the graph. The node is found using the hgid.
	 * @param db The Neo4j graph object
	 * @param hgid The hgid of the node that is to be removed.
	 * @return An array of {@literal Map<String, String>} objects containing the parameters of each relationship that was removed in the process,
	 * or null if none were removed.
	 * @throws IOException Thrown if no node with this hgid was present in the graph.
	 */
	public static Map<String, String>[] deleteNode(GraphDatabaseService db, String hgid) throws IOException {		
		// Verify node exists and get it
		Node node = GraphMethods.getNodeByHgid(db, hgid);
		if (node == null) throw new IOException("Node with " + HistographTokens.General.HGID + " '" + hgid + "' not found in graph.");
		ArrayList<Map<String, String>> relParamList = new ArrayList<Map<String, String>>();
		
		try (Transaction tx = db.beginTx()) {
			// Remove all associated relationships
			Iterable<Relationship> relationships = node.getRelationships();
			for (Relationship rel : relationships) {
				Map<String, String> relParams = new HashMap<String, String>();
				
				relParams.put(HistographTokens.RelationTokens.FROM, rel.getStartNode().getProperty(HistographTokens.General.HGID).toString());
				relParams.put(HistographTokens.RelationTokens.TO, rel.getEndNode().getProperty(HistographTokens.General.HGID).toString());
				relParams.put(HistographTokens.RelationTokens.LABEL, RelationType.fromRelationshipType(rel.getType()).getLabel());
				relParams.put(HistographTokens.General.SOURCE, rel.getProperty(HistographTokens.General.SOURCE).toString());
				
				relParamList.add(relParams);
				
				rel.delete();
			}
			
			// Remove node
			node.delete();
			tx.success();
		}
		
		if (relParamList.size() > 0) {
			@SuppressWarnings("unchecked")
			Map<String, String>[] relMaps = new Map[relParamList.size()];
			relMaps = relParamList.toArray(relMaps);
			return relMaps;
		} else {
			return null;
		}
	}
	
	/**
	 * Looks up all nodes given a URI or hgid parameter. First, nodes are looked up by URI -- if no nodes are found, then
	 * a node is looked up by the hgid.
	 * @param db The Neo4j graph object
	 * @param hgidOrURI A parameter representing either a URI or a hgid.
	 * @return An array of the nodes found. If no nodes are found, null is returned.
	 * @throws IOException Thrown if multiple nodes are found <b>when searching by hgid</b>. This should not happen due to the uniqueness constraint.
	 */
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
	
	/**
	 * Creates one or more relationships between nodes, depending on the hgid or URI parameter that are passed.
	 * First, all start and end nodes are looked up (possibly more than one if URI's are passed), and if the
	 * specified relationship between these nodes do not exist, they are created and returned.
	 * @param db The Neo4j graph object
	 * @param params A map containing the parameters for the relationships that are to be created. Typically created by
	 * the {@link org.waag.histograph.util.InputReader} class, returned with {@link org.waag.histograph.queue.Task#getParams()}.
	 * @return An array containing all newly created Relationships, or null if no relations were created 
	 * @throws IOException Thrown if no nodes are found based on the given hgid's or URI's.
	 * @throws RejectedException Thrown if a hgid or URI is not present in the graph. Can be caught to 
	 */
	public static Relationship[] addRelation(GraphDatabaseService db, Map<String, String> params) throws IOException, RejectedException {
		// Search on URI first, return all found nodes in array
		Node[] fromNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) throw new RejectedException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.", params.get(HistographTokens.RelationTokens.FROM), params);
		if (toNodes == null) throw new RejectedException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.", params.get(HistographTokens.RelationTokens.TO), params);
		
		RelationshipType relType = RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL));
		String source = params.get(HistographTokens.General.SOURCE);
		ArrayList<Relationship> relArray = new ArrayList<Relationship>();
		
		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				// Verify the absence of the new relation, skip to next if already present
				if (!GraphMethods.relationAbsent(db, fromNode, toNode, relType, source)) continue;

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
	
	/**
	 * Updates a relationship. Currently not supported -- an IOException is always thrown.
	 * @param db The Neo4j graph object
	 * @param params A map containing the parameters for the relationship that is to be updated. Typically created by
	 * the {@link org.waag.histograph.util.InputReader} class, returned with {@link org.waag.histograph.queue.Task#getParams()}.
	 * @throws IOException Always thrown as updating relationships is not supported. Remove and add a new relationship instead.
	 */
	public static void updateRelation(GraphDatabaseService db, Map<String, String> params) throws IOException {
		throw new IOException("Updating relationships not supported.");
	}
	
	/**
	 * Deletes one or more relationships, based on a relationship label, a source and node identifiers (either hgid or URI).
	 * @param db The Neo4j graph object
	 * @param params A map containing the parameters for the relationship that is to be deleted. Typically created by
	 * the {@link org.waag.histograph.util.InputReader} class, returned with {@link org.waag.histograph.queue.Task#getParams()}.
	 * @throws IOException Thrown if no nodes are present associated with the provided hgid or URI.
	 */
	public static void deleteRelation(GraphDatabaseService db, Map<String, String> params) throws IOException {
		// Search on URI first, return all found nodes in array
		Node[] fromNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = getNodesFromParams(db, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.");
		if (toNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.");
		
		RelationshipType relType = RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL));
		String source = params.get(HistographTokens.General.SOURCE);
		
		// For all possible node pairs, find the relationship between them and delete when found
		boolean relationshipsRemoved = false;
		
		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				// Verify the existence of the relation
				Relationship r = GraphMethods.getRelation(db, fromNode, toNode, relType, source);
				if (r == null) continue;
				
				// Remove relation between nodes
				try (Transaction tx = db.beginTx()) {
					r.delete();
					tx.success();
				}
				
				relationshipsRemoved = true;
			}
		}
		
		if (!relationshipsRemoved) {
			String relationName = params.get(HistographTokens.RelationTokens.FROM) + " --" + relType.name() + "--> " + params.get(HistographTokens.RelationTokens.TO);
			throw new IOException("No relationships '" + relationName + "' from source '" + source + "' found in graph.");
		}
	}
	
	/**
	 * Returns a node given a hgid, or null if no nodes with this hgid are found.
	 * @param db The Neo4j graph object
	 * @param hgid The hgid to search for.
	 * @return The node associated with the specified hgid, or null if no such node is found.
	 * @throws IOException Thrown if multiple nodes with the specified hgid are found. This should not happen due to the uniqueness constraint.
	 */
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
	
	/**
	 * Checks whether a node with a specified hgid exists.
	 * @param db The Neo4j graph object
	 * @param hgid The hgid to search for.
	 * @return A boolean value indicating if a node with the specified hgid exists in the graph.
	 * @throws IOException Thrown if multiple nodes with the specified hgid exist.
	 */
	public static boolean nodeExistsByHgid(GraphDatabaseService db, String hgid) throws IOException {
		return (getNodeByHgid(db, hgid) != null);
	}
	
	/**
	 * Checks whether a node with a specified hgid does NOT exist in the graph.
	 * @param db The Neo4j graph object
	 * @param hgid The hgid to search for.
	 * @return A boolean value indicating if a node with the specified hgid is absent in the graph.
	 * @throws IOException Thrown if multiple nodes with the specified hgid exist.
	 */
	public static boolean nodeAbsentByHgid(GraphDatabaseService db, String hgid) throws IOException {
		return (getNodeByHgid(db, hgid) == null);
	}
	
	/**
	 * Find all nodes containing a specified URI.
	 * @param db The Neo4j graph object
	 * @param uri The URI to search for.
	 * @return An array of nodes containing this URI, or null if no such nodes are present.
	 */
	public static Node[] getNodesByURI(GraphDatabaseService db, String uri) {
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
	
	/**
	 * Finds and returns a relationship based on two nodes, a relationship type and the source it is specified by.
	 * @param db The Neo4j graph object
	 * @param fromNode The start node object of the relationship
	 * @param toNode The end node object of the relationship
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return The relationship associated with the parameters, or null if this relationship does not exist.
	 */
	public static Relationship getRelation(GraphDatabaseService db, Node fromNode, Node toNode, RelationshipType type, String source) {
		try (Transaction tx = db.beginTx()) {
			for (Relationship r : fromNode.getRelationships(type, Direction.OUTGOING)) {
				if (r.getEndNode().equals(toNode) && r.getProperty(HistographTokens.General.SOURCE).equals(source)) {
					return r;
				}
			}
		}
		return null;
	}
	
	/**
	 * Checks whether a relationship specified by a specific source exists in the graph.
	 * @param db The Neo4j graph object
	 * @param fromNode The start node object of the relationship
	 * @param toNode The end node object of the relationship
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return A boolean value indicating whether this relationship exists in the graph.
	 */
	public static boolean relationExists(GraphDatabaseService db, Node fromNode, Node toNode, RelationshipType type, String source) {
		return (getRelation(db, fromNode, toNode, type, source) != null);
	}
	
	/**
	 * Checks whether a relationship specified by a specific source is absent in the graph.
	 * @param db The Neo4j graph object
	 * @param fromNode The start node object of the relationship
	 * @param toNode The end node object of the relationship
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return A boolean value indicating whether this relationship is absent in the graph.
	 */
	public static boolean relationAbsent(GraphDatabaseService db, Node fromNode, Node toNode, RelationshipType type, String source) {
		return (getRelation(db, fromNode, toNode, type, source) == null);
	}
}