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
import org.waag.histograph.util.HistographTokens.PITIdentifyingMethod;

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
	 * @return The newly created Node.
	 * @throws ConstraintViolationException Thrown if a node with the same hgid was already present.
	 */
	public static Node addNode(GraphDatabaseService db, Map<String, String> params) throws ConstraintViolationException {		
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
			return newPIT;
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
	
	// TODO Test properly! Write Javadocs!
	public static String getNodePropertyByMethod(GraphDatabaseService db, PITIdentifyingMethod method, Node node) throws IOException {
		try (Transaction tx = db.beginTx()) {
			switch (method) {
			case HGID:
				return node.getProperty(HistographTokens.General.HGID).toString();
			case URI:
				return node.getProperty(HistographTokens.PITTokens.URI).toString();
			default:
				throw new IOException("Failed to fetch node property.");
			}
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

				PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.valueOf(rel.getProperty(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD).toString());
				PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.valueOf(rel.getProperty(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD).toString());
				
				switch (fromIdMethod) {
				case HGID:
					String fromHgid = rel.getStartNode().getProperty(HistographTokens.General.HGID).toString();
					relParams.put(HistographTokens.RelationTokens.FROM, fromHgid);
					if (hgid.equals(fromHgid)) {
						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE, fromHgid);
						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD, PITIdentifyingMethod.HGID.toString());
					}
					break;
				case URI:
					String fromURI = rel.getStartNode().getProperty(HistographTokens.PITTokens.URI).toString();
					String nodeURI = node.getProperty(HistographTokens.PITTokens.URI).toString();
					relParams.put(HistographTokens.RelationTokens.FROM, fromURI);
					if (nodeURI.equals(fromURI)) {
						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE, fromURI);
						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD, PITIdentifyingMethod.URI.toString());
					}
					break;
				default:
					throw new IOException("Unexpected PIT identifying method found while deleting relationship associated with PIT");
				}
				
				switch (toIdMethod) {
				case HGID:
					String toHgid = rel.getEndNode().getProperty(HistographTokens.General.HGID).toString();
					relParams.put(HistographTokens.RelationTokens.TO, toHgid);
					if (hgid.equals(toHgid)) {
						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE, toHgid);
						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD, PITIdentifyingMethod.HGID.toString());
					}
					break;
				case URI:
					String toURI = rel.getEndNode().getProperty(HistographTokens.PITTokens.URI).toString();

//					temporary fix for https://github.com/histograph/core/issues/40
//					String nodeURI = node.getProperty(HistographTokens.PITTokens.URI).toString();
					relParams.put(HistographTokens.RelationTokens.TO, toURI);
//					if (nodeURI.equals(toURI)) {
//						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE, toURI);
//						relParams.put(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD, PITIdentifyingMethod.URI.toString());
//					}
					break;
				default:
					throw new IOException("Unexpected PIT identifying method found while deleting relationship associated with PIT");
				}
				
				relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
				relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
				relParams.put(HistographTokens.RelationTokens.LABEL, RelationType.fromRelationshipType(rel.getType()).toString());
				relParams.put(HistographTokens.General.SOURCEID, rel.getProperty(HistographTokens.General.SOURCEID).toString());
								
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
	 * Looks up all nodes given a URI or hgid parameter and the corresponding parameter type.
	 * @param db The Neo4j graph object
	 * @param method The method in which is specified whether the hgidOrURI parameter is a URI or an hgid.
	 * @param hgidOrURI A string representing either a URI or an hgid.
	 * @return An array of the nodes found. If no nodes are found, null is returned.
	 * @throws IOException Thrown if either an unexpected method is supplied, or if multiple nodes are
	 * found <b>when searching by hgid</b>. The latter should not happen due to the uniqueness constraint.
	 */
	public static Node[] getNodesByIdMethod(GraphDatabaseService db, PITIdentifyingMethod method, String hgidOrURI) throws IOException {
		if (method == null) throw new IOException("Unexpected PIT identifying method received: null");
		switch (method) {
		case HGID:
			Node[] nodes = new Node[1];
			nodes[0] = GraphMethods.getNodeByHgid(db, hgidOrURI);
			if (nodes[0] == null) return null;
			return nodes;
		case URI:
			return GraphMethods.getNodesByUri(db, hgidOrURI);
		default:
			throw new IOException("Unexpected PIT identifying method received: " + method);
		}
	}
	
	/**
	 * Creates one or more relationships between nodes, depending on the hgid or URI parameter that are passed.
	 * First, all start and end nodes are looked up (possibly more than one if URI's are passed), and if the
	 * specified relationship between these nodes do not exist, they are created and returned.
	 * @param db The Neo4j graph object
	 * @param params A map containing the parameters for the relationships that are to be created. Typically created by
	 * the {@link org.waag.histograph.util.InputReader} class, returned with {@link org.waag.histograph.queue.Task#getParams()}.
	 * @return An array containing all newly created Relationships, or null if no relations were created 
	 * @throws IOException Thrown if multiple nodes were found when searching on HGID or if the params contain an unexpected PITIdentifyingMethod.
	 * @throws RejectedRelationNotification Thrown if a hgid or URI is not present in the graph. Can be caught to add the relation to the rejected relations DB.
	 */
	public static Relationship[] addRelation(GraphDatabaseService db, Map<String, String> params) throws IOException, RejectedRelationNotification {
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD));
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD));
		
		// Search on URI first, return all found nodes in array
		Node[] fromNodes = getNodesByIdMethod(db, fromIdMethod, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = getNodesByIdMethod(db, toIdMethod, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) {
			params.put(HistographTokens.RelationTokens.REJECTION_CAUSE, params.get(HistographTokens.RelationTokens.FROM));
			params.put(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD, fromIdMethod.toString());
			throw new RejectedRelationNotification("No nodes with " + fromIdMethod + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.", params);
		}
		if (toNodes == null) {
			params.put(HistographTokens.RelationTokens.REJECTION_CAUSE, params.get(HistographTokens.RelationTokens.TO));
			params.put(HistographTokens.RelationTokens.REJECTION_CAUSE_ID_METHOD, toIdMethod.toString());
			throw new RejectedRelationNotification("No nodes with " + toIdMethod + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.", params);
		}
		
		RelationshipType relType = RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL));
		String source = params.get(HistographTokens.General.SOURCEID);
		ArrayList<Relationship> relArray = new ArrayList<Relationship>();
		
		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				// Verify the absence of the new relation, skip to next if already present
				if (!GraphMethods.relationAbsent(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source)) continue;

				// Create relation between nodes
				try (Transaction tx = db.beginTx()) {

					// once in the normal direction
					Relationship rel = fromNode.createRelationshipTo(toNode, relType);
					rel.setProperty(HistographTokens.General.SOURCEID, params.get(HistographTokens.General.SOURCEID));
					rel.setProperty(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
					rel.setProperty(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
					relArray.add(rel);

					// check if we are adding an undirected relation
					if(RelationType.SAMEHGCONCEPT.name().equals(relType.label()))
					{
						// 	add edge in other direction if it doesn't already exists
						if (GraphMethods.relationAbsent(db, toNode, toIdMethod, fromNode, fromIdMethod, relType, source))
						{
							Relationship opposite = toNode.createRelationshipTo(fromNode, relType);
							opposite.setProperty(HistographTokens.General.SOURCEID, params.get(HistographTokens.General.SOURCEID));
							opposite.setProperty(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, toIdMethod.toString());
							opposite.setProperty(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, fromIdMethod.toString());

							// record
							relArray.add(opposite);
						}
					}

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
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD));
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD));
		
		// Search on URI first, return all found nodes in array
		Node[] fromNodes = getNodesByIdMethod(db, fromIdMethod, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = getNodesByIdMethod(db, toIdMethod, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.");
		if (toNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.");
		
		RelationshipType relType = RelationType.fromLabel(params.get(HistographTokens.RelationTokens.LABEL));
		String source = params.get(HistographTokens.General.SOURCEID);
		
		// For all possible node pairs, find the relationship between them and delete when found
		boolean relationshipsRemoved = false;
		
		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				// Verify the existence of the relation
				Relationship r = GraphMethods.getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source);
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
        Node[] nodes = getNodesByProperty(db, HistographTokens.General.HGID, hgid);
        if (nodes == null) return null;
        if (nodes.length > 1) throw new IOException ("Multiple nodes with " + HistographTokens.General.HGID + " '" + hgid + "' found in graph.");
        return nodes[0];
	}	
	
	/**
	 * Checks whether a node with a specified hgid exists.
	 * @param db The Neo4j graph object
	 * @param hgid The hgid to search for.
	 * @return A boolean value indicating if a node with the specified hgid exists in the graph.
	 * @throws IOException Thrown if multiple nodes with the specified hgid exist. Should never happen due to uniqueness constraint.
	 */
	public static boolean nodeExistsByHgid(GraphDatabaseService db, String hgid) throws IOException {
		return (getNodeByHgid(db, hgid) != null);
	}
	
	/**
	 * Checks whether a node with a specified hgid does NOT exist in the graph.
	 * @param db The Neo4j graph object
	 * @param hgid The hgid to search for.
	 * @return A boolean value indicating if a node with the specified hgid is absent in the graph.
	 * @throws IOException Thrown if multiple nodes with the specified hgid exist. Should never happen due to uniqueness constraint.
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
	public static Node[] getNodesByUri(GraphDatabaseService db, String uri) {
		return getNodesByProperty(db, HistographTokens.PITTokens.URI, uri);
	}
	
	/**
	 * Checks whether a node with a specified URI exists.
	 * @param db The Neo4j graph object
	 * @param uri The URI to search for.
	 * @return A boolean value indicating if a node with the specified URI exists in the graph.
	 */
	public static boolean nodeExistsByUri(GraphDatabaseService db, String uri) {
		return (getNodesByUri(db, uri) != null);
	}
	
	/**
	 * Checks whether a node with a specified URI does NOT exist in the graph.
	 * @param db The Neo4j graph object
	 * @param uri The URI to search for.
	 * @return A boolean value indicating if a node with the specified URI is absent in the graph.
	 */
	public static boolean nodeAbsentByUri(GraphDatabaseService db, String uri) {
		return (getNodesByUri(db, uri) == null);
	}
	
	/**
	 * Find all nodes containing a specified URI.
	 * @param db The Neo4j graph object
	 * @param property The property key to search on.
	 * @param value The property value to search for.
	 * @return An array of nodes containing this URI, or null if no such nodes are present.
	 */
	public static Node[] getNodesByProperty(GraphDatabaseService db, String property, String value) {
		try (Transaction ignored = db.beginTx()) {
			try (ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, property, value).iterator()) {
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
 	 * @param fromIdMethod The method in which is specified whether the fromNode parameter is a URI or an hgid.
	 * @param toNode The end node object of the relationship
	 * @param toIdMethod The method in which is specified whether the toNode parameter is a URI or an hgid.
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return The relationship associated with the parameters, or null if this relationship does not exist.
	 */
	public static Relationship getRelation(GraphDatabaseService db, Node fromNode, PITIdentifyingMethod fromIdMethod, Node toNode, PITIdentifyingMethod toIdMethod, RelationshipType type, String source) {
		try (Transaction tx = db.beginTx()) {
			for (Relationship r : fromNode.getRelationships(type, Direction.OUTGOING)) {
				if (r.getEndNode().equals(toNode) 
						&& r.getProperty(HistographTokens.General.SOURCEID).equals(source)
						&& (PITIdentifyingMethod.valueOf(r.getProperty(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD).toString()) == fromIdMethod) 
						&& (PITIdentifyingMethod.valueOf(r.getProperty(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD).toString()) == toIdMethod)) {
					return r;
				}
			}
		}
		return null;
	}
	
	/**
	 * Finds and returns a relationship based on two node identifiers (HGID / URI) and the other relationship parameters.
	 * @param db The Neo4j graph object
	 * @param fromNodeHgidOrUri The start node identifier of the relationship
 	 * @param fromIdMethod The method in which is specified whether the fromNode parameter is a URI or an hgid.
	 * @param toNodeHgidOrUri The end node identifier of the relationship
	 * @param toIdMethod The method in which is specified whether the toNode parameter is a URI or an hgid.
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return The relationships associated with the parameters, or null if no relationships were found.
	 * @throws IOException Thrown if either an unexpected method is supplied, or if multiple nodes are
	 * found <b>when searching by hgid</b>. The latter should not happen due to the uniqueness constraint.
	 */
	// TODO Test properly!
	public static Relationship[] getRelations(GraphDatabaseService db, String fromNodeHgidOrUri, PITIdentifyingMethod fromIdMethod, String toNodeHgidOrUri, PITIdentifyingMethod toIdMethod, RelationshipType type, String source) throws IOException {
		ArrayList<Relationship> list = new ArrayList<Relationship>();
		
		Node[] fromNodes = getNodesByIdMethod(db, fromIdMethod, fromNodeHgidOrUri);
		Node[] toNodes = getNodesByIdMethod(db, toIdMethod, toNodeHgidOrUri);
		
		if (fromNodes == null || toNodes == null) return null;
		
		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				Relationship r = getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, type, source);
				if (r != null) list.add(r);
			}
		}
		
		if (list.isEmpty()) return null;
		Relationship[] out = new Relationship[list.size()];
		return list.toArray(out);
	}
	
	/**
	 * Checks whether one or more relationships with specific parameters exist in the graph.
	 * @param db The Neo4j graph object
	 * @param fromNodeHgidOrUri The start node identifier of the relationship
	 * @param fromIdMethod The method in which is specified whether the fromNode parameter is a URI or an hgid.
	 * @param toNodeHgidOrUri The end node identifier of the relationship
	 * @param toIdMethod The method in which is specified whether the toNode parameter is a URI or an hgid.
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return A boolean value indicating whether this relationship exists in the graph.
	 * @throws IOException Thrown if either an unexpected method is supplied, or if multiple nodes are
	 * found <b>when searching by hgid</b>. The latter should not happen due to the uniqueness constraint.
	 */
	// TODO Test properly!
	public static boolean relationsExist(GraphDatabaseService db, String fromNodeHgidOrUri, PITIdentifyingMethod fromIdMethod, String toNodeHgidOrUri, PITIdentifyingMethod toIdMethod, RelationshipType type, String source) throws IOException {
		return (getRelations(db, fromNodeHgidOrUri, fromIdMethod, toNodeHgidOrUri, toIdMethod, type, source) != null);
	}
	
	/**
	 * Checks whether one or more relationships with specific parameters do not exist in the graph.
	 * @param db The Neo4j graph object
	 * @param fromNodeHgidOrUri The start node identifier of the relationship
	 * @param fromIdMethod The method in which is specified whether the fromNode parameter is a URI or an hgid.
	 * @param toNodeHgidOrUri The end node identifier of the relationship
	 * @param toIdMethod The method in which is specified whether the toNode parameter is a URI or an hgid.
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return A boolean value indicating whether this relationship exists in the graph.
	 * @throws IOException Thrown if either an unexpected method is supplied, or if multiple nodes are
	 * found <b>when searching by hgid</b>. The latter should not happen due to the uniqueness constraint.
	 */
	// TODO Test properly!
	public static boolean relationsAbsent(GraphDatabaseService db, String fromNodeHgidOrUri, PITIdentifyingMethod fromIdMethod, String toNodeHgidOrUri, PITIdentifyingMethod toIdMethod, RelationshipType type, String source) throws IOException {
		return (getRelations(db, fromNodeHgidOrUri, fromIdMethod, toNodeHgidOrUri, toIdMethod, type, source) == null);
	}
	
	/**
	 * Checks whether a relationship specified by a specific source exists in the graph.
	 * @param db The Neo4j graph object
	 * @param fromNode The start node object of the relationship
	 * @param fromIdMethod The method in which is specified whether the fromNode parameter is a URI or an hgid.
	 * @param toNode The end node object of the relationship
	 * @param toIdMethod The method in which is specified whether the toNode parameter is a URI or an hgid.
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return A boolean value indicating whether this relationship exists in the graph.
	 */
	public static boolean relationExists(GraphDatabaseService db, Node fromNode, PITIdentifyingMethod fromIdMethod, Node toNode, PITIdentifyingMethod toIdMethod, RelationshipType type, String source) {
		return (getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, type, source) != null);
	}
	
	/**
	 * Checks whether a relationship specified by a specific source is absent in the graph.
	 * @param db The Neo4j graph object
	 * @param fromNode The start node object of the relationship
	 * @param fromIdMethod The method in which is specified whether the fromNode parameter is a URI or an hgid.
	 * @param toNode The end node object of the relationship
	 * @param toIdMethod The method in which is specified whether the toNode parameter is a URI or an hgid.
	 * @param type The relationship type of this relationship
	 * @param source The source of the relationship
	 * @return A boolean value indicating whether this relationship is absent in the graph.
	 */
	public static boolean relationAbsent(GraphDatabaseService db, Node fromNode, PITIdentifyingMethod fromIdMethod, Node toNode, PITIdentifyingMethod toIdMethod, RelationshipType type, String source) {
		return (getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, type, source) == null);
	}
}