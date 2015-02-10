package org.waag.histograph.inferencing;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.TreeMap;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.structure.io.graphson.LegacyGraphSONReader;

public class Main {
	
	public static final String DATA_FOLDER = "/Users/rutger/Desktop/waag/git/historische-geocoder/data";

	public static final String[] GRAPHSON_DATA_SETS = {"tgn",
											  "geonames",
											  "bag", 
											  "gemeentegeschiedenis", 
											  "ilvb", 
											  "militieregisters", 
											  "pleiades", 
											  "poorterboeken", 
											  "simon-hart",
											  "verdwenen-dorpen",
	                                          "voc-opvarenden"};
	
	TreeMap<String, String> inverseRelationsMap;
	TreeMap<String, ArrayList<String>> atomicRelationsMap;
	Neo4jGraph g;
	
	public Main () {
		g = Neo4jGraph.open("/tmp/neo4j");
		g.tx().open();
		initializeMaps();
	}
	
	public void initializeMaps() {
		inverseRelationsMap = new TreeMap<String, String>();
		atomicRelationsMap = new TreeMap<String, ArrayList<String>>();
		
		inverseRelationsMap.put("hg:sameAs",  "hg:sameAs");
		inverseRelationsMap.put("hg:liesIn", "hg:contains");
		inverseRelationsMap.put("hg:contains", "hg:liesIn");
		
		ArrayList<String> sameAsList = new ArrayList<String>();
		sameAsList.add("hg:conceptIdentical");
		sameAsList.add("hg:typeIdentical");
		atomicRelationsMap.put("hg:sameAs", sameAsList);
		
		ArrayList<String> absorbedList = new ArrayList<String>();
		absorbedList.add("hg:conceptIn");
		absorbedList.add("hg:periodBefore");
		absorbedList.add("hg:typeIdentical");
		absorbedList.add("hg:geometryIntersects");
		atomicRelationsMap.put("hg:absorbed", absorbedList);		
		
		ArrayList<String> isUsedForList = new ArrayList<String>();
		isUsedForList.add("hg:conceptIdentical");
		isUsedForList.add("hg:typeIdentical");
		atomicRelationsMap.put("hg:isUsedFor", isUsedForList);
		
		ArrayList<String> liesInList = new ArrayList<String>();
		liesInList.add("hg:geometryIntersects");
		atomicRelationsMap.put("hg:liesIn", liesInList);
	}	
	
	public void importDatasets() throws Exception {
		System.out.println("Importing data sets...");
		for (String i : GRAPHSON_DATA_SETS) {
			File file = new File(DATA_FOLDER + "/" + i + "/" + i + ".graphson.json");
			FileInputStream stream = new FileInputStream(file);
			LegacyGraphSONReader.Builder r = LegacyGraphSONReader.build();
			r.create().readGraph(stream, g);
//			g.io().le(DATA_FOLDER + "/" + i + "/" + i + ".graphson.json");
		}
	}
	
//	public int inferAtomicRelations() throws Exception {
//		String source = "inferredAtomicRelationEdge";
//		int roundCount, inferredCount = 0;
//		do {
//			Iterator<Vertex> vertices = g.V();
//			roundCount = 0;
//			
//			while (vertices.hasNext()) {
//				Vertex v1 = vertices.next();
//				
//				for (Iterator<String> i = atomicRelationsMap.keySet().iterator(); i.hasNext(); ) {
//					String relation = i.next();
//					GremlinPipeline<Vertex, Vertex> pipe1 = new GremlinPipeline<Vertex, Vertex>();
//					
//					pipe1.start(v1).out(relation);
//					while (pipe1.hasNext()) {
//						Vertex v2 = pipe1.next();
//												
//						for (Iterator<String> i2 = atomicRelationsMap.get(relation).iterator(); i2.hasNext(); ) {
//							String atomicRelation = i2.next();
//							GremlinPipeline<Vertex, Vertex> pipe2 = new GremlinPipeline<Vertex, Vertex>();
//							
//							pipe2.start(v1).out(atomicRelation);
//							boolean alreadyExists = false;
//							while (pipe2.hasNext()) {
//								if (pipe2.next().equals(v2)) {
//									alreadyExists = true;
//									break;
//								}
//							}
//							if (!alreadyExists) {
//								Edge newEdge = g.addEdge(null, v1, v2, atomicRelation);
//								newEdge.setProperty("source", source);
//								newEdge.setProperty("uri", atomicRelation + "-" + source + "-" + v1.getProperty("uri") + "-" + v2.getProperty("uri"));
//								roundCount++;
//							}
//						}
//					}
//				}
//			}
//			
//			inferredCount += roundCount;
//		} while (roundCount > 0);
//		System.out.println("  " + inferredCount + " atomic relation edges inferred.");
//		return inferredCount;
//	}
//	
//	public int inferTransitiveSameAs1() throws Exception {
//		String source = "inferredTransitiveSameAsEdge";
//		int roundCount, inferredCount = 0;
//		do {
//			Iterator<Vertex> vertices = g.getVertices().iterator();
//			roundCount = 0;
//			while (vertices.hasNext()) {
//				Vertex v1 = vertices.next();
//				GremlinPipeline<Vertex, Edge> pipe1 = new GremlinPipeline<Vertex, Edge>();
//				pipe1.start(v1).out("hg:sameAs").outE();
//				while (pipe1.hasNext()) {
//					Edge e = pipe1.next();
//					String edgeLabel = e.getLabel();
//					if (!(edgeLabel.equals("hg:sameAs"))) {
//						GremlinPipeline<Edge, Vertex> pipe2 = new GremlinPipeline<Edge, Vertex>();
//						pipe2.start(e).inV();
//						
//						while (pipe2.hasNext()) {
//							Vertex v3 = pipe2.next();
//							GremlinPipeline<Vertex, Vertex> pipe3 = new GremlinPipeline<Vertex, Vertex>();
//							pipe3.start(v1).out(edgeLabel);
//							
//							boolean alreadyExists = false;
//							while (pipe3.hasNext()) {
//								if (pipe3.next().equals(v3)) {
//									alreadyExists = true;
//									break;
//								}
//							}
//							if (!alreadyExists) {
//								Edge newEdge = g.addEdge(null, v1, v3, edgeLabel);
//								newEdge.setProperty("source", source);
//								newEdge.setProperty("uri", edgeLabel + "-" + source + "-" + v1.getProperty("uri") + "-" + v3.getProperty("uri"));
//								roundCount++;
//							}
//						}
//					}
//				}
//			}
//			inferredCount += roundCount;
//		} while (roundCount > 0);
//		System.out.println("  " + inferredCount + " transitive hg:sameAs edges (1) inferred.");
//		return inferredCount;
//	}
//	
//	public int inferTransitiveSameAs2() throws Exception {
//		String source = "inferredTransitiveSameAsEdge";
//		int roundCount, inferredCount = 0;
//		do {
//			Iterator<Vertex> vertices = g.getVertices().iterator();
//			roundCount = 0;
//			while (vertices.hasNext()) {
//				Vertex v2 = vertices.next();
//				GremlinPipeline<Vertex, Vertex> pipe1 = new GremlinPipeline<Vertex, Vertex>();
//				pipe1.start(v2).out("hg:sameAs");
//				while (pipe1.hasNext()) {
//					Vertex v3 = pipe1.next();
//					GremlinPipeline<Vertex, Edge> pipe2 = new GremlinPipeline<Vertex, Edge>();
//					pipe2.start(v2).inE();
//					
//					while (pipe2.hasNext()) {
//						Edge e = pipe2.next();
//						String edgeLabel = e.getLabel();
//						if (!(edgeLabel.equals("hg:sameAs"))) {
//							GremlinPipeline<Edge, Vertex> pipe3 = new GremlinPipeline<Edge, Vertex>();
//							pipe3.start(e).outV();
//							
//							while (pipe3.hasNext()) {
//								Vertex v1 = pipe3.next();
//								
//								GremlinPipeline<Vertex, Vertex> pipe4 = new GremlinPipeline<Vertex, Vertex>();
//								pipe4.start(v1).out(edgeLabel);
//								
//								boolean alreadyExists = false;
//								while (pipe4.hasNext()) {
//									if (pipe4.next().equals(v3)) {
//										alreadyExists = true;
//										break;
//									}
//								}
//								if (!alreadyExists) {
//									Edge newEdge = g.addEdge(null, v1, v3, edgeLabel);
//									newEdge.setProperty("source", source);
//									newEdge.setProperty("uri", edgeLabel + "-" + source + "-" + v1.getProperty("uri") + "-" + v3.getProperty("uri"));
//									roundCount++;
//								}
//							}
//						}
//					}
//				}
//			}
//			inferredCount += roundCount;
//		} while (roundCount > 0);
//		System.out.println("  " + inferredCount + " transitive hg:sameAs edges (2) inferred.");
//		return inferredCount;
//	}
//	
//	public int inferInverseEdges() throws Exception {
//		String source = "inferredInverseEdge";
//		int inferredCount = 0;
//
//		for (Iterator<String> i = inverseRelationsMap.keySet().iterator(); i.hasNext(); ) {
//			String forwardEdge = i.next();
//			String inverseEdge = inverseRelationsMap.get(forwardEdge);
//			
//			Iterator<Vertex> vertices = g.getVertices().iterator();
//			while (vertices.hasNext()) {
//				Vertex v1 = vertices.next();
//				GremlinPipeline<Vertex, Vertex> pipe1 = new GremlinPipeline<Vertex, Vertex>();
//				pipe1.start(v1).outE(forwardEdge).inV();
//				while (pipe1.hasNext()) {
//					Vertex v2 = pipe1.next();
//					GremlinPipeline<Vertex, Vertex> pipe2 = new GremlinPipeline<Vertex, Vertex>();
//					pipe2.start(v2).outE(inverseEdge).inV();
//					boolean alreadyExists = false;
//					while (pipe2.hasNext()) {
//						if (pipe2.next().equals(v1)) {
//							alreadyExists = true;
//							break;
//						}
//					}
//					if (!alreadyExists) {
//						Edge e = g.addEdge(null, v2, v1, inverseEdge);
//						e.setProperty("source", source);
//						e.setProperty("uri", inverseEdge + "-" + source + "-" + v2.getProperty("uri") + "-" + v1.getProperty("uri"));
//						inferredCount++;
//					}
//				}
//			}
//		}
//		System.out.println("  " + inferredCount + " inverse edges inferred.");
//		return inferredCount;
//	}
//	
////	public void setupIndices() throws Exception {
////		System.out.println("Creating index...");
////		TitanManagement mgmt = g.getManagementSystem();
////		PropertyKey uri = mgmt.makePropertyKey("uri").dataType(String.class).make();
////		mgmt.buildIndex("byUri", Vertex.class).addKey(uri).unique().buildCompositeIndex();
////		PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
////		mgmt.buildIndex("byName", Vertex.class).addKey(name, Mapping.TEXT.getParameter()).buildMixedIndex("search");
////		mgmt.commit();
////	}
//	
//	public void clearGraph() throws Exception {
//		System.out.println("Clearing graph...");
//
//		for (Iterator<Vertex> i = g.getVertices().iterator(); i.hasNext(); ) {
//			Vertex v = i.next();
//			g.removeVertex(v);
//		}
//		
//		for (Iterator<Edge> i = g.getEdges().iterator(); i.hasNext(); ) {
//			Edge e = i.next();
//			g.removeEdge(e);
//		}
//		
//		if (g.getVertices().iterator().hasNext() || g.getEdges().iterator().hasNext()) throw new Exception("Graph not empty after clearing.");
//	}
	
	public void start() {
		try {
//			clearGraph();
//			setupIndices();
			importDatasets();
			
			int inferredEdges, roundCount, roundNumber;
			inferredEdges = roundCount = roundNumber = 0;
			
			do {
				System.out.println("Starting inferencing round " + ++roundNumber + "...");
				roundCount = 0;
//				roundCount += inferAtomicRelations();
//				roundCount += inferTransitiveSameAs1();
//				roundCount += inferTransitiveSameAs2();
				inferredEdges += roundCount;
			} while (roundCount > 0);
			
//			System.out.println("Total #inferred edges: " + inferredEdges);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("Shutting down.");
			try {
				g.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] argv) {
		new Main().start();
	}
}