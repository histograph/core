package org.waag.histograph.graph;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;
import org.waag.histograph.reasoner.ReasoningDefinitions;
import org.waag.histograph.reasoner.ReasoningDefinitions.RelationType;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.HistographTokens.PITIdentifyingMethod;

public class GraphMethodsTest {
	
	public static GraphDatabaseService db;
	
	@ClassRule
	public static TemporaryFolder dbpath = new TemporaryFolder();
	
	@BeforeClass // Setup database at the start of this class.
	public static void setUpDatabase () {
		String filePath;
		try {
			filePath = dbpath.newFolder().getAbsolutePath();
			db = new GraphDatabaseFactory().newEmbeddedDatabase(filePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Initialize uniqueness constraint for HGID property
		try (Transaction tx = db.beginTx()) {
			Schema schema = db.schema();
			if (!schema.getConstraints(ReasoningDefinitions.NodeType.PIT).iterator().hasNext()) {
				schema.constraintFor(ReasoningDefinitions.NodeType.PIT).assertPropertyIsUnique(HistographTokens.General.HGID).create();
			}
			tx.success();
		}
		
		try (Transaction tx = db.beginTx()) {
		    Schema schema = db.schema();
		    schema.awaitIndexesOnline(10, TimeUnit.MINUTES);
		    tx.success();
		}
	}
	
	@After // Clear the graph after each test.
	public void clearGraph () throws Exception {
		try (Transaction tx = db.beginTx()) {
			GlobalGraphOperations ggo = GlobalGraphOperations.at(db);
			for (Node n : ggo.getAllNodes()) {
				for (Relationship r : n.getRelationships()) {
					r.delete();
				}
				n.delete();
			}
			tx.success();
		} catch (Exception e) {
			throw new Exception("Failed to clean graph after test!", e);
		}
	}
	
	@AfterClass // Shut down after all tests are done.
	public static void shutdownDatabase () {
		db.shutdown();
	}

	@Test
	public void addNode_IfNotPresent_ShouldSuccesfullyAdd () {
		Map<String, String> params = new HashMap<String, String>();
		String[] placeholders = {"ab", "cd", "ef", "gh", "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"};
		
		for (int i=0; i<placeholders.length; i+=2) {
			params.put(placeholders[i], placeholders[i+1]);
		}
		
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to an empty graph.");
		}
		
		try (Transaction tx = db.beginTx()) {
			// Find the node using one of the parameter values
			ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, placeholders[0], placeholders[1]).iterator();
			Node n = i.next();
			
			// Make sure all parameters were set correctly
			for (Map.Entry<String, String> entry : params.entrySet()) {
				assertEquals("Key/value pair not correctly set while adding node.", entry.getValue(), n.getProperty(entry.getKey()).toString());
			}
		} catch (NoSuchElementException e) {
			fail("Failed to retrieve the node created in the test.");
		}
	}
	
	@Test (expected=ConstraintViolationException.class)
	public void addNode_IfAlreadyPresent_ShouldThrowException () throws ConstraintViolationException {
		Map<String, String> params = new HashMap<String, String>();
		String[] placeholders = {HistographTokens.General.HGID, "hgidValue", "ef", "gh", "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"};

		for (int i=0; i<placeholders.length; i+=2) {
			params.put(placeholders[i], placeholders[i+1]);
		}

		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to an empty graph.");
		}

		// A ConstraintViolationException should be thrown as the parameters violate the uniqueness property of the HGID key
		GraphMethods.addNode(db, params);
	}

	@Test
	public void updateNode_IfNodePresent_ShouldSuccessfullyUpdate () {
		Map<String, String> firstParams = new HashMap<String, String>();
		Map<String, String> secondParams = new HashMap<String, String>();
		String hgid = "hgidValue";
		String[] firstKeyVals = {HistographTokens.General.HGID, hgid, "ab", "cd", "ef", "gh", "ij", "oldVal"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid, "ef", "gh", "ij", "newVal", "mn", "op"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add node with first parameters. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Update node (note: HGID is the same in both parameter maps) with second parameters.
		try {
			GraphMethods.updateNode(db, secondParams);
		} catch (IOException e) {
			fail("Failed to update node in graph.");
		}
		
		try (Transaction tx = db.beginTx()) {
			// Fetch node based on HGID.
			Node node = null;
			try (ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, HistographTokens.General.HGID, hgid).iterator()) {
				if (!i.hasNext()) throw new IOException();
				node = i.next();
			} catch (IOException e) {
				fail("Failed to fetch node from graph.");
			}
			
			// Test updated values
			
			// Key/value pair that was the same before updating
			try {
				assertEquals("gh", node.getProperty("ef").toString());
			} catch (NotFoundException e) {
				fail("Failed to retrieve node property.");
			}
			
			// Key/value pair removed during update
			try {
				node.getProperty("ab");
				fail("Retrieved node property that should have been removed during update.");
			} catch (NotFoundException e) {}
			
			// Key/value pair with an updated value
			try {
				assertEquals("newVal", node.getProperty("ij").toString());
			} catch (NotFoundException e) {
				fail("Failed to retrieve node property with a new value after the update.");
			}
			
			// Key/value pair added during update
			try {
				assertEquals("op", node.getProperty("mn").toString());
			} catch (NotFoundException e) {
				fail("Failed to retrieve node property that should have been added during update.");
			}
		}	
	}
	
	@Test (expected=IOException.class)
	public void updateNode_IfNotPresent_ShouldThrowException () throws IOException {
		Map<String, String> dummyParams = new HashMap<String, String>();
		String hgid = "hgidValue";
		String[] firstKeyVals = {HistographTokens.General.HGID, hgid, "ab", "cd", "ef", "gh", "ij", "oldVal"};
		
		// Populate parameter map
		for (int i=0; i<firstKeyVals.length; i+=2) {
			dummyParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}

		// Should throw an IOException as no nodes with this HGID currently exist (empty graph)
		GraphMethods.updateNode(db, dummyParams);
	}
	
	@Test
	public void deleteNode_NoAssociatedRelationships_ShouldSuccessfullyDelete () {
		Map<String, String> dummyParams = new HashMap<String, String>();
		String hgid = "hgidValue";
		String[] firstKeyVals = {HistographTokens.General.HGID, hgid, "ab", "cd", "ef", "gh"};
		
		// Populate parameter map
		for (int i=0; i<firstKeyVals.length; i+=2) {
			dummyParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}

		// Add node to graph. Assumes the addNode method works properly.
		try {
			GraphMethods.addNode(db, dummyParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		try {
			Map<String, String>[] deletedRels = GraphMethods.deleteNode(db, hgid);
			assertNull("Null was expected from deleteNode as no relationships were removed.", deletedRels);
		} catch (IOException e) {
			fail("Failed to delete node from graph -- node not found in the first place.");
		}
				
		try (Transaction tx = db.beginTx()) {
			// Find the node using one of the parameter values
			ResourceIterator<Node> i = db.findNodesByLabelAndProperty(ReasoningDefinitions.NodeType.PIT, HistographTokens.General.HGID, hgid).iterator();
			i.next();
			fail("Failed to delete node from graph -- the node still exists after deletion.");
		} catch (NoSuchElementException e) {}
	}
	
	@Test (expected=IOException.class)
	public void deleteNode_NodeNotPresent_ShouldThrowException () throws IOException {
		String hgid = "dummyValue";
		
		// Should throw IOException because this node does not exist (empty graph)
		GraphMethods.deleteNode(db, hgid);
	}
	
	@Test
	public void deleteNode_WithAssociatedRelationships_ShouldReturnRelationships () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		Map<String, String> thirdNodeParams = new HashMap<String, String>();
		Map<String, String> fourthNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";
		String hgid3 = "hgid3";
		String hgid4 = "hgid4";
		String uri1 = "uri1";
		String uri2 = "uri2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, HistographTokens.PITTokens.URI, uri1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, HistographTokens.PITTokens.URI, uri1, "ef", "gh"};
		String[] thirdKeyVals = {HistographTokens.General.HGID, hgid3, HistographTokens.PITTokens.URI, uri2, "ij", "kl"};
		String[] fourthKeyVals = {HistographTokens.General.HGID, hgid4, HistographTokens.PITTokens.URI, uri2, "mn", "op"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		for (int i=0; i<thirdKeyVals.length; i+=2) {
			thirdNodeParams.put(thirdKeyVals[i], thirdKeyVals[i+1]);
		}
		
		for (int i=0; i<fourthKeyVals.length; i+=2) {
			fourthNodeParams.put(fourthKeyVals[i], fourthKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
			GraphMethods.addNode(db, thirdNodeParams);
			GraphMethods.addNode(db, fourthNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, uri1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.RelationTokens.TO, uri2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		relParams.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		// Add relationships between a URI-URI pair. Four different relationships should be created.
		try {
			GraphMethods.addRelation(db, relParams);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
		
		// Delete one of the end nodes. Removes 2 relationships in the process.
		try {
			Map<String, String>[] deletedRelParams = GraphMethods.deleteNode(db, hgid4);
			assertEquals("Two relationships should have been returned.", 2, deletedRelParams.length);
			
			for (Map<String, String> map : deletedRelParams) {
				assertEquals("Different FROM param was returned.", map.get(HistographTokens.RelationTokens.FROM), relParams.get(HistographTokens.RelationTokens.FROM));
				assertEquals("Different FROM_ID_METHOD param was returned.", map.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD), relParams.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD));
				assertEquals("Different TO param was returned.", map.get(HistographTokens.RelationTokens.TO), relParams.get(HistographTokens.RelationTokens.TO));
				assertEquals("Different TO_ID_METHOD param was returned.", map.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD), relParams.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD));
				assertEquals("Different SOURCEID param was returned.", map.get(HistographTokens.General.SOURCEID), relParams.get(HistographTokens.General.SOURCEID));
				assertEquals("Different LABEL param was returned.", map.get(HistographTokens.RelationTokens.LABEL), relParams.get(HistographTokens.RelationTokens.LABEL));				
			}
			
		} catch (IOException e) {
			fail("Exception thrown when deleting node.");
		}
	}
	
	@Test
	public void getNodesByProperty_SingleNodeFoundWithMultipleProperties_ShouldReturnSameNode () {
		Map<String, String> params = new HashMap<String, String>();
		String[] placeholders = {"ab", "cd", "ef", "gh", "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"};
		
		for (int i=0; i<placeholders.length; i+=2) {
			params.put(placeholders[i], placeholders[i+1]);
		}

		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to the empty graph.");
		}

		int nKeyValPairs = params.entrySet().size();
		Node[] nodes = new Node[nKeyValPairs];
		
		// Retrieve the node using all key/value pairs
		for (int i=0; i<nKeyValPairs; i++) {
			String key = placeholders[i*2];
			String val = placeholders[i*2+1];
			Node[] resultNodes = GraphMethods.getNodesByProperty(db, key, val);
			assertEquals("Failed to fetch node from graph with parameter", 1, resultNodes.length);
			nodes[i] = resultNodes[0];
		}
		
		// Extra check for all nodes to be equal
		for (int i=1; i<nodes.length; i++) {
			assertEquals("Two different nodes found -- should be equal", nodes[i], nodes[i-1]);
		}
	}
	
	@Test
	public void getNodesByProperty_PropertiesOverlap_ShouldReturnMultipleNodes () {
		String[][] nodeKeyVals = { 	{"ab", "cd", "ef", "gh", "ij", "val1"},
									{"ab", "cd", "ef", "gh", "ij", "val2"},
									{"ab", "cd", "ef", "gh"},
									{"ab", "cd"} 
								 };
		for (int i=0; i<nodeKeyVals.length; i++) {
			Map<String, String> params = new HashMap<String, String>();
			for (int j=0; j<nodeKeyVals[i].length; j+=2) {
				params.put(nodeKeyVals[i][j], nodeKeyVals[i][j+1]);
			}
			try {
				GraphMethods.addNode(db, params);
			} catch (ConstraintViolationException e) {
				fail("Failed to add node to the empty graph.");
			}
		}
		
		Node[] resultNodes;
		
		// Retrieve the nodes using all key/value pairs
		resultNodes = GraphMethods.getNodesByProperty(db, "ab", "cd");
		assertEquals("Failed to fetch nodes from graph with parameter ab", 4, resultNodes.length);
		
		resultNodes = GraphMethods.getNodesByProperty(db, "ef", "gh");
		assertEquals("Failed to fetch nodes from graph with parameter ef", 3, resultNodes.length);
		
		resultNodes = GraphMethods.getNodesByProperty(db, "ij", "val1");
		assertEquals("Failed to fetch nodes from graph with parameter ij", 1, resultNodes.length);
		
		resultNodes = GraphMethods.getNodesByProperty(db, "ij", "val2");
		assertEquals("Failed to fetch nodes from graph with parameter ij", 1, resultNodes.length);
		
		resultNodes = GraphMethods.getNodesByProperty(db, "ij", "val3");
		assertNull("Did not receive null after search to invalid key/val pair", resultNodes);
	}
	
	@Test
	public void getNodeByHgid_IfPresent_ShouldReturnNode () {
		Map<String, String> params = new HashMap<String, String>();
		String hgid = "hgidValue";
		String uri = "uriValue";
		String[] placeholders = {HistographTokens.General.HGID, hgid, HistographTokens.PITTokens.URI, uri, "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"};
		
		for (int i=0; i<placeholders.length; i+=2) {
			params.put(placeholders[i], placeholders[i+1]);
		}
		
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to the empty graph.");
		}

		try {
			Node node = GraphMethods.getNodeByHgid(db, hgid);
			assertNotNull("Failed to fetch node from graph with HGID parameter", node);
		} catch (IOException e) {
			fail("IOException should only be thrown when duplicate HGID's are found.");
		}
	}
	
	@Test
	public void getNodeByHgid_IfNotPresent_ShouldReturnNull () {
		String hgid = "dummyHgid";
		// Null should be returned as this node does not exist in the graph
		try {
			Node node = GraphMethods.getNodeByHgid(db, hgid);
			assertNull("Null should have been returned as the node was not present in the graph.", node);
		} catch (IOException e) {
			fail("IOException should only be thrown when duplicate HGID's are found.");
		}
	}
	
	@Test
	public void getNodesByUri_IfNotPresent_ShouldReturnNull () {
		String uri = "dummyURI";

		Node[] node = GraphMethods.getNodesByUri(db, uri);
		assertNull("Null should have been returned as the node was not present in the graph.", node);
	}
	
	@Test
	public void getNodesByUri_IfPresent_ShouldReturnNodes () {
		// Add some PITs to the graph with the same URI value
		String[] hgids = {"hgidValue", "anotherValue", "yetAnotherValue"};
		String uri = "uriValue";
		String[] keyVals = {HistographTokens.PITTokens.URI, uri, "ij", "kl", "mn", "op"};
		
		for (int i=0; i<hgids.length; i++) {
			Map<String, String> params = new HashMap<String, String>();
			params.put(HistographTokens.General.HGID, hgids[i]);
			for (int j=0; j<keyVals.length; j+=2) {
				params.put(keyVals[j], keyVals[j+1]);	
			}
			try {
				GraphMethods.addNode(db, params);
			} catch (ConstraintViolationException e) {
				fail("Failed to add node to the graph.");
			}
		}
		Node[] nodes = GraphMethods.getNodesByUri(db, uri);
		assertEquals("Failed to fetch nodes from graph by URI", hgids.length, nodes.length);
	}
	
	@Test (expected=IOException.class)
	public void getNodesByUri_IfInvalidIdMethod_ShouldThrowException () throws IOException {
		Map<String, String> params = new HashMap<String, String>();
		String hgid = "hgidValue";
		String uri = "uriValue";
		String[] placeholders = {HistographTokens.General.HGID, hgid, HistographTokens.PITTokens.URI, uri, "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"};
		
		for (int i=0; i<placeholders.length; i+=2) {
			params.put(placeholders[i], placeholders[i+1]);
		}
		
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to an empty graph.");
		}

		// Invalid identifying method provided (null), IOException should therefore be thrown
		GraphMethods.getNodesByIdMethod(db, null, hgid);
	}

	@Test
	public void getNodesByIdMethod_IfHgidNotPresent_ShouldReturnNull () {
		String hgid = "hgidValue";

		// GetNodes from parameters. No nodes found --> null should be returned.
		try {
			Node[] nodes = GraphMethods.getNodesByIdMethod(db, PITIdentifyingMethod.HGID, hgid);
			assertNull("No null returned although the node was not found.", nodes);
		} catch (IOException e) {
			fail("IOException thrown -- should not have happened.");
		}
	}
	
	@Test
	public void getNodesByIdMethod_IfUriNotPresent_ShouldReturnNull () {
		String uri = "uriValue";

		// GetNodes from parameters. No nodes found --> null should be returned.
		try {
			Node[] nodes = GraphMethods.getNodesByIdMethod(db, PITIdentifyingMethod.URI, uri);
			assertNull("No null returned although the node was not found.", nodes);
		} catch (IOException e) {
			fail("IOException thrown -- should not have happened.");
		}
	}
	
	@Test
	public void getNodesByIdMethod_IfHgidPresent_ShouldReturnSingleNode () {
		Map<String, String> params = new HashMap<String, String>();
		String hgid = "hgidValue";
		String uri = "uriValue";
		String[] placeholders = {HistographTokens.General.HGID, hgid, HistographTokens.PITTokens.URI, uri, "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"};
		
		for (int i=0; i<placeholders.length; i+=2) {
			params.put(placeholders[i], placeholders[i+1]);
		}
		
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to the empty graph.");
		}

		Node[] nodes = null;

		// Invalid identifying method provided (null), IOException should therefore be thrown
		try {
			nodes = GraphMethods.getNodesByIdMethod(db, PITIdentifyingMethod.HGID, hgid);
		} catch (IOException e) {
			fail("Failed to get node from HGID parameter.");
		}
		
		assertEquals("Failed to fetch node from graph through HGID parameter", 1, nodes.length);
	}
	
	@Test
	public void getNodesByIdMethod_IfUriPresent_ShouldReturnNodes () {
		// Add some PITs to the graph with the same URI value
		String[] hgids = {"hgidValue", "anotherValue", "yetAnotherValue"};
		String uri = "uriValue";
		String[] keyVals = {HistographTokens.PITTokens.URI, uri, "ij", "kl", "mn", "op"};
		
		for (int i=0; i<hgids.length; i++) {
			Map<String, String> params = new HashMap<String, String>();
			params.put(HistographTokens.General.HGID, hgids[i]);
			for (int j=0; j<keyVals.length; j+=2) {
				params.put(keyVals[j], keyVals[j+1]);	
			}
			try {
				GraphMethods.addNode(db, params);
			} catch (ConstraintViolationException e) {
				fail("Failed to add node to the graph.");
			}
		}
		Node[] nodes = GraphMethods.getNodesByUri(db, uri);
		assertEquals("Failed to fetch nodes from graph by URI", hgids.length, nodes.length);

		// Fetch nodes by URI
		try {
			nodes = GraphMethods.getNodesByIdMethod(db, PITIdentifyingMethod.URI, uri);
		} catch (IOException e) {
			fail("Failed to get node from HGID parameter.");
		}
		
		assertEquals("Failed to fetch node from graph through HGID parameter", 3, nodes.length);
	}
	
	@Test
	public void addRelation_successfulAddByHgidHgid_shouldReturnOneRelation () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, hgid1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.HGID.toString());
		relParams.put(HistographTokens.RelationTokens.TO, hgid2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.HGID.toString());
		relParams.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		relParams.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		// Add relationship, here based on the HGID values
		try {
			Relationship[] rels = GraphMethods.addRelation(db, relParams);
			assertEquals("One relationship expected as return value after its creation", 1, rels.length);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
	}
	
	@Test
	public void addRelation_successfulAddbyHgidUri_shouldReturnMultipleRelations () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		Map<String, String> thirdNodeParams = new HashMap<String, String>();
		Map<String, String> fourthNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";
		String hgid3 = "hgid3";
		String hgid4 = "hgid4";
		String uri1 = "uri1";
		String uri2 = "uri2";
		String uri3 = "uri3";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, HistographTokens.PITTokens.URI, uri1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, HistographTokens.PITTokens.URI, uri2, "ef", "gh"};
		String[] thirdKeyVals = {HistographTokens.General.HGID, hgid3, HistographTokens.PITTokens.URI, uri2, "ij", "kl"};
		String[] fourthKeyVals = {HistographTokens.General.HGID, hgid4, HistographTokens.PITTokens.URI, uri3, "mn", "op"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		for (int i=0; i<thirdKeyVals.length; i+=2) {
			thirdNodeParams.put(thirdKeyVals[i], thirdKeyVals[i+1]);
		}
		
		for (int i=0; i<fourthKeyVals.length; i+=2) {
			fourthNodeParams.put(fourthKeyVals[i], fourthKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
			GraphMethods.addNode(db, thirdNodeParams);
			GraphMethods.addNode(db, fourthNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> rel1Params = new HashMap<String, String>();
		Map<String, String> rel2Params = new HashMap<String, String>();
		
		rel1Params.put(HistographTokens.RelationTokens.FROM, hgid1);
		rel1Params.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.HGID.toString());
		rel1Params.put(HistographTokens.RelationTokens.TO, uri2);
		rel1Params.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		rel1Params.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		rel1Params.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		rel2Params.put(HistographTokens.RelationTokens.FROM, uri2);
		rel2Params.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		rel2Params.put(HistographTokens.RelationTokens.TO, hgid4);
		rel2Params.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.HGID.toString());
		rel2Params.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		rel2Params.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.LIES_IN.toString());

		Relationship[] rels;
		// Add relationships, first to a HGID-URI pair, then to a URI-HGID pair
		try {
			rels = GraphMethods.addRelation(db, rel1Params);
			assertEquals("Two relationships expected as return value after its creation", 2, rels.length);
			assertNotEquals("Two different relationships expected as return value.", rels[0], rels[1]);
			
			rels = GraphMethods.addRelation(db, rel2Params);
			assertEquals("Two relationships expected as return value after its creation", 2, rels.length);
			assertNotEquals("Two different relationships expected as return value.", rels[0], rels[1]);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
	}
	
	@Test
	public void addRelation_successfulAddbyUriUri_shouldReturnMultipleRelations () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		Map<String, String> thirdNodeParams = new HashMap<String, String>();
		Map<String, String> fourthNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";
		String hgid3 = "hgid3";
		String hgid4 = "hgid4";
		String uri1 = "uri1";
		String uri2 = "uri2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, HistographTokens.PITTokens.URI, uri1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, HistographTokens.PITTokens.URI, uri1, "ef", "gh"};
		String[] thirdKeyVals = {HistographTokens.General.HGID, hgid3, HistographTokens.PITTokens.URI, uri2, "ij", "kl"};
		String[] fourthKeyVals = {HistographTokens.General.HGID, hgid4, HistographTokens.PITTokens.URI, uri2, "mn", "op"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		for (int i=0; i<thirdKeyVals.length; i+=2) {
			thirdNodeParams.put(thirdKeyVals[i], thirdKeyVals[i+1]);
		}
		
		for (int i=0; i<fourthKeyVals.length; i+=2) {
			fourthNodeParams.put(fourthKeyVals[i], fourthKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
			GraphMethods.addNode(db, thirdNodeParams);
			GraphMethods.addNode(db, fourthNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, uri1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.RelationTokens.TO, uri2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		relParams.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		Relationship[] rels;
		// Add relationships between a URI-URI pair. Four different relationships should be created.
		try {
			rels = GraphMethods.addRelation(db, relParams);
			assertEquals("Two relationships expected as return value after its creation", 4, rels.length);
			
			for (int i=0; i<rels.length; i++) {
				for (int j=0; j<rels.length; j++) {
					if (i != j) assertNotEquals("Two different relationships expected as return value.", rels[i], rels[j]);

				}
			}
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
	}

	@Test (expected=RejectedRelationNotification.class)
	public void addRelation_unsuccessfulAddByHgidHgid_shouldThrowException () throws RejectedRelationNotification {
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, "hgid1");
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.HGID.toString());
		relParams.put(HistographTokens.RelationTokens.TO, "hgid2");
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.HGID.toString());
		relParams.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		relParams.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		// Add relationship, here based on the HGID values
		try {
			GraphMethods.addRelation(db, relParams);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		}
	}
	
	@Test
	public void nodeExistsByHgid_IfNodeNotPresent_shouldReturnFalse () {
		String hgid = "dummyId";
		try {
			assertFalse("Node does not exist, wrong output returned.", GraphMethods.nodeExistsByHgid(db, hgid));
		} catch (IOException e) {
			fail("IOException should not have been thrown.");
		}
	}
	
	@Test
	public void nodeExistsByHgid_IfNodePresent_shouldReturnTrue () {
		Map<String, String> params = new HashMap<String, String>();
		String hgid = "hgidValue";
		String[] keyVals = {HistographTokens.General.HGID, hgid, "ab", "cd"};
		
		// Populate parameter map
		for (int i=0; i<keyVals.length; i+=2) {
			params.put(keyVals[i], keyVals[i+1]);
		}
		
		// Add node with parameters. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		try {
			assertTrue("Node does exist, wrong output returned.", GraphMethods.nodeExistsByHgid(db, hgid));
		} catch (IOException e) {
			fail("IOException should not have been thrown.");
		}
	}
	
	@Test
	public void nodeAbsentByHgid_IfNodeNotPresent_shouldReturnTrue () {
		String hgid = "dummyId";
		try {
			assertTrue("Node does not exist, wrong output returned.", GraphMethods.nodeAbsentByHgid(db, hgid));
		} catch (IOException e) {
			fail("IOException should not have been thrown.");
		}
	}
	
	@Test
	public void nodeAbsentByHgid_IfNodePresent_shouldReturnFalse () {
		Map<String, String> params = new HashMap<String, String>();
		String hgid = "hgidValue";
		String[] keyVals = {HistographTokens.General.HGID, hgid, "ab", "cd"};
		
		// Populate parameter map
		for (int i=0; i<keyVals.length; i+=2) {
			params.put(keyVals[i], keyVals[i+1]);
		}
		
		// Add node with parameters. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		try {
			assertFalse("Node does exist, wrong output returned.", GraphMethods.nodeAbsentByHgid(db, hgid));
		} catch (IOException e) {
			fail("IOException should not have been thrown.");
		}
	}
	
	@Test
	public void nodeExistsByUri_IfNodeNotPresent_shouldReturnFalse () {
		String uri = "dummyURI";
		assertFalse("Node does not exist, wrong output returned.", GraphMethods.nodeExistsByUri(db, uri));
	}
	
	@Test
	public void nodeExistsByUri_IfNodePresent_shouldReturnTrue () {
		Map<String, String> params = new HashMap<String, String>();
		String uri = "uriValue";
		String[] keyVals = {HistographTokens.PITTokens.URI, uri, "ab", "cd"};
		
		// Populate parameter map
		for (int i=0; i<keyVals.length; i+=2) {
			params.put(keyVals[i], keyVals[i+1]);
		}
		
		// Add node with parameters. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		assertTrue("Node does exist, wrong output returned.", GraphMethods.nodeExistsByUri(db, uri));
	}
	
	@Test
	public void nodeExistsByUri_IfMultipleNodesPresent_shouldReturnTrue () {
		Map<String, String> firstParams = new HashMap<String, String>();
		Map<String, String> secondParams = new HashMap<String, String>();
		String uri = "uriValue";
		String[] firstKeyVals = {HistographTokens.PITTokens.URI, uri, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.PITTokens.URI, uri, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add node with parameters. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstParams);
			GraphMethods.addNode(db, secondParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		assertTrue("Node does exist, wrong output returned.", GraphMethods.nodeExistsByUri(db, uri));
	}
	
	@Test
	public void nodeAbsentByUri_IfNodeNotPresent_shouldReturnTrue () {
		String uri = "dummyURI";
		assertTrue("Node does not exist, wrong output returned.", GraphMethods.nodeAbsentByUri(db, uri));
	}
	
	@Test
	public void nodeAbsentByUri_IfNodePresent_shouldReturnFalse () {
		Map<String, String> params = new HashMap<String, String>();
		String uri = "uriValue";
		String[] keyVals = {HistographTokens.PITTokens.URI, uri, "ab", "cd"};
		
		// Populate parameter map
		for (int i=0; i<keyVals.length; i+=2) {
			params.put(keyVals[i], keyVals[i+1]);
		}
		
		// Add node with parameters. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, params);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		assertFalse("Node does exist, wrong output returned.", GraphMethods.nodeAbsentByUri(db, uri));
	}
	
	@Test
	public void nodeAbsentByUri_IfMultipleNodesPresent_shouldReturnFalse () {
		Map<String, String> firstParams = new HashMap<String, String>();
		Map<String, String> secondParams = new HashMap<String, String>();
		String uri = "uriValue";
		String[] firstKeyVals = {HistographTokens.PITTokens.URI, uri, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.PITTokens.URI, uri, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add node with parameters. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstParams);
			GraphMethods.addNode(db, secondParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		assertFalse("Node does exist, wrong output returned.", GraphMethods.nodeAbsentByUri(db, uri));
	}
		
	@Test
	public void getRelation_IfSearchWithDifferentParams_shouldReturnNull () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.HGID;
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.HGID;
		String source = "rutgervanwilligen";
		RelationType relType = ReasoningDefinitions.RelationType.ABSORBED;
		
		relParams.put(HistographTokens.RelationTokens.FROM, hgid1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
		relParams.put(HistographTokens.RelationTokens.TO, hgid2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
		relParams.put(HistographTokens.General.SOURCEID, source);
		relParams.put(HistographTokens.RelationTokens.LABEL, relType.toString());
		
		// Add relationship, assumes addRelation works properly
		try {
			GraphMethods.addRelation(db, relParams);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
		
		Node fromNode = null;
		Node toNode = null;
		// Lookup both nodes, assumes getNodeByHgid works
		try {
			fromNode = GraphMethods.getNodeByHgid(db, hgid1);
			toNode = GraphMethods.getNodeByHgid(db, hgid2);
		} catch (IOException e) {
			fail("Failed to lookup nodes that were just created.");
		}
		
		Relationship rel;
		
		rel = GraphMethods.getRelation(db, fromNode, PITIdentifyingMethod.URI, toNode, toIdMethod, relType, source);
		assertNull("Relationship found while it should not have been.", rel);
		rel = GraphMethods.getRelation(db, fromNode, fromIdMethod, toNode, PITIdentifyingMethod.URI, relType, source);
		assertNull("Relationship found while it should not have been.", rel);
		rel = GraphMethods.getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, "bertspaan");
		assertNull("Relationship found while it should not have been.", rel);
		rel = GraphMethods.getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, RelationType.CONTAINS, source);
		assertNull("Relationship found while it should not have been.", rel);
	}
	
	@Test
	public void getRelation_IfPresent_shouldReturnSingleRelation () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.HGID;
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.HGID;
		String source = "rutgervanwilligen";
		RelationType relType = ReasoningDefinitions.RelationType.ABSORBED;
		
		relParams.put(HistographTokens.RelationTokens.FROM, hgid1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
		relParams.put(HistographTokens.RelationTokens.TO, hgid2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
		relParams.put(HistographTokens.General.SOURCEID, source);
		relParams.put(HistographTokens.RelationTokens.LABEL, relType.toString());
		
		// Add relationship, assumes addRelation works properly
		try {
			GraphMethods.addRelation(db, relParams);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
		
		Node fromNode = null;
		Node toNode = null;
		// Lookup both nodes, assumes getNodeByHgid works
		try {
			fromNode = GraphMethods.getNodeByHgid(db, hgid1);
			toNode = GraphMethods.getNodeByHgid(db, hgid2);
		} catch (IOException e) {
			fail("Failed to lookup nodes that were just created.");
		}
		
		Relationship rel = GraphMethods.getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source);
		assertNotNull("Relationship not found while it should have been.", rel);
	}
	
	@Test (expected=IOException.class)
	public void deleteRelation_IfNotPresent_shouldThrowException () throws IOException {
		// Create new relationship parameters of a relationship that does not exist
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, "uri1");
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.RelationTokens.TO, "uri2");
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		relParams.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		// Delete them using the same parameters.
		GraphMethods.deleteRelation(db, relParams);
	}
	
	@Test
	public void deleteRelation_IfPresent_shouldDeleteRelation () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		Map<String, String> thirdNodeParams = new HashMap<String, String>();
		Map<String, String> fourthNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";
		String hgid3 = "hgid3";
		String hgid4 = "hgid4";
		String uri1 = "uri1";
		String uri2 = "uri2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, HistographTokens.PITTokens.URI, uri1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, HistographTokens.PITTokens.URI, uri1, "ef", "gh"};
		String[] thirdKeyVals = {HistographTokens.General.HGID, hgid3, HistographTokens.PITTokens.URI, uri2, "ij", "kl"};
		String[] fourthKeyVals = {HistographTokens.General.HGID, hgid4, HistographTokens.PITTokens.URI, uri2, "mn", "op"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		for (int i=0; i<thirdKeyVals.length; i+=2) {
			thirdNodeParams.put(thirdKeyVals[i], thirdKeyVals[i+1]);
		}
		
		for (int i=0; i<fourthKeyVals.length; i+=2) {
			fourthNodeParams.put(fourthKeyVals[i], fourthKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
			GraphMethods.addNode(db, thirdNodeParams);
			GraphMethods.addNode(db, fourthNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters to add and delete, and a dummy relationship to keep
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, uri1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.RelationTokens.TO, uri2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams.put(HistographTokens.General.SOURCEID, "rutgervanwilligen");
		relParams.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		Map<String, String> relParams2 = new HashMap<String, String>();
		
		relParams2.put(HistographTokens.RelationTokens.FROM, uri1);
		relParams2.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams2.put(HistographTokens.RelationTokens.TO, uri2);
		relParams2.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, PITIdentifyingMethod.URI.toString());
		relParams2.put(HistographTokens.General.SOURCEID, "bertspaan");
		relParams2.put(HistographTokens.RelationTokens.LABEL, ReasoningDefinitions.RelationType.ABSORBED.toString());
		
		// Add relationships between a URI-URI pair. Eight different relationships should be created.
		try {
			GraphMethods.addRelation(db, relParams);
			GraphMethods.addRelation(db, relParams2);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
		
		// Delete four of them using the first parameters.
		try {
			GraphMethods.deleteRelation(db, relParams);
		} catch (IOException e) {
			fail("IOException thrown while deleting relations, should not happen");
		}
		
		// Check if deleted successfully and if the other four still remain.
		try (Transaction tx = db.beginTx()) {
			GlobalGraphOperations ggo = GlobalGraphOperations.at(db);
			Iterator<Relationship> i = ggo.getAllRelationships().iterator();
			int relCount = 0;
			
			while (i.hasNext()) {
				i.next();
				relCount++;
			}
			assertEquals("Four relationships should be left", 4, relCount);
		}
	}
	
	@Test
	public void relationExists_IfNotPresent_shouldReturnFalse () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";
		String source = "rutgervanwilligen";
		RelationType relType = RelationType.ABSORBED;
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.HGID;
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.HGID;

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, hgid1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
		relParams.put(HistographTokens.RelationTokens.TO, hgid2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
		relParams.put(HistographTokens.General.SOURCEID, source);
		relParams.put(HistographTokens.RelationTokens.LABEL, relType.toString());
		
		Node fromNode = null;
		Node toNode = null;
		// Lookup both nodes, assumes getNodeByHgid works
		try {
			fromNode = GraphMethods.getNodeByHgid(db, hgid1);
			toNode = GraphMethods.getNodeByHgid(db, hgid2);
		} catch (IOException e) {
			fail("Failed to lookup nodes that were just created.");
		}
		
		assertFalse(GraphMethods.relationExists(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source));
	}
	
	@Test
	public void relationExists_IfPresent_shouldReturnTrue () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.HGID;
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.HGID;
		String source = "rutgervanwilligen";
		RelationType relType = ReasoningDefinitions.RelationType.ABSORBED;
		
		relParams.put(HistographTokens.RelationTokens.FROM, hgid1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
		relParams.put(HistographTokens.RelationTokens.TO, hgid2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
		relParams.put(HistographTokens.General.SOURCEID, source);
		relParams.put(HistographTokens.RelationTokens.LABEL, relType.toString());
		
		// Add relationship, assumes addRelation works properly
		try {
			GraphMethods.addRelation(db, relParams);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
		
		Node fromNode = null;
		Node toNode = null;
		// Lookup both nodes, assumes getNodeByHgid works
		try {
			fromNode = GraphMethods.getNodeByHgid(db, hgid1);
			toNode = GraphMethods.getNodeByHgid(db, hgid2);
		} catch (IOException e) {
			fail("Failed to lookup nodes that were just created.");
		}
		
		assertTrue("Relationship not found while it should have been.", GraphMethods.relationExists(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source));
	}
	
	@Test
	public void relationAbsent_IfNotPresent_shouldReturnTrue () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";
		String source = "rutgervanwilligen";
		RelationType relType = RelationType.ABSORBED;
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.HGID;
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.HGID;

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		relParams.put(HistographTokens.RelationTokens.FROM, hgid1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
		relParams.put(HistographTokens.RelationTokens.TO, hgid2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
		relParams.put(HistographTokens.General.SOURCEID, source);
		relParams.put(HistographTokens.RelationTokens.LABEL, relType.toString());
		
		Node fromNode = null;
		Node toNode = null;
		// Lookup both nodes, assumes getNodeByHgid works
		try {
			fromNode = GraphMethods.getNodeByHgid(db, hgid1);
			toNode = GraphMethods.getNodeByHgid(db, hgid2);
		} catch (IOException e) {
			fail("Failed to lookup nodes that were just created.");
		}
		
		assertTrue(GraphMethods.relationAbsent(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source));
	}
	
	@Test
	public void relationAbsent_IfPresent_shouldReturnFalse () {
		Map<String, String> firstNodeParams = new HashMap<String, String>();
		Map<String, String> secondNodeParams = new HashMap<String, String>();
		String hgid1 = "hgid1";
		String hgid2 = "hgid2";

		String[] firstKeyVals = {HistographTokens.General.HGID, hgid1, "ab", "cd"};
		String[] secondKeyVals = {HistographTokens.General.HGID, hgid2, "ef", "gh"};
		
		// Populate parameter maps
		for (int i=0; i<firstKeyVals.length; i+=2) {
			firstNodeParams.put(firstKeyVals[i], firstKeyVals[i+1]);
		}
		
		for (int i=0; i<secondKeyVals.length; i+=2) {
			secondNodeParams.put(secondKeyVals[i], secondKeyVals[i+1]);
		}
		
		// Add nodes. The addNode method has been tested already
		try {
			GraphMethods.addNode(db, firstNodeParams);
			GraphMethods.addNode(db, secondNodeParams);
		} catch (ConstraintViolationException e) {
			fail("Failed to add node to graph.");
		}
		
		// Create new relationship parameters
		Map<String, String> relParams = new HashMap<String, String>();
		
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.HGID;
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.HGID;
		String source = "rutgervanwilligen";
		RelationType relType = ReasoningDefinitions.RelationType.ABSORBED;
		
		relParams.put(HistographTokens.RelationTokens.FROM, hgid1);
		relParams.put(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD, fromIdMethod.toString());
		relParams.put(HistographTokens.RelationTokens.TO, hgid2);
		relParams.put(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD, toIdMethod.toString());
		relParams.put(HistographTokens.General.SOURCEID, source);
		relParams.put(HistographTokens.RelationTokens.LABEL, relType.toString());
		
		// Add relationship, assumes addRelation works properly
		try {
			GraphMethods.addRelation(db, relParams);
		} catch (IOException e) {
			fail("Failed to add relationship to graph during HGID lookup phase.");
		} catch (RejectedRelationNotification e) {
			fail("Failed to add relationship -- relationship rejected.");
		}
		
		Node fromNode = null;
		Node toNode = null;
		// Lookup both nodes, assumes getNodeByHgid works
		try {
			fromNode = GraphMethods.getNodeByHgid(db, hgid1);
			toNode = GraphMethods.getNodeByHgid(db, hgid2);
		} catch (IOException e) {
			fail("Failed to lookup nodes that were just created.");
		}
		
		assertFalse("Relationship not found while it should have been.", GraphMethods.relationAbsent(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, source));
	}
	
	@Test (expected=IOException.class)
	public void testUpdateRelationship () throws IOException {
		Map<String, String> params = new HashMap<String, String>();
		GraphMethods.updateRelation(db, params);
	}
}