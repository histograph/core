package org.waag.histograph.graph;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.waag.histograph.queue.QueueTask;
import org.waag.histograph.reasoner.AtomicInferencer;
import org.waag.histograph.util.HistographTokens;

public class GraphThread implements Runnable {
	
	GraphDatabaseService db;
	ExecutionEngine engine;
	BlockingQueue<QueueTask> queue;
	boolean verbose;
	
	public GraphThread (GraphDatabaseService db, BlockingQueue<QueueTask> queue, boolean verbose) {
		this.db = db;
		engine = new ExecutionEngine(db);
		this.queue = queue;
		this.verbose = verbose;
	}
	
	public void run () {
		try {
			int tasksDone = 0;
			while (true) {
				QueueTask task = queue.take();
				
				try {
					performAction(task);
				} catch (IOException e) {
					writeToFile("graphErrors.txt", "Error: ", e.getMessage());
				} catch (ConstraintViolationException e) {
					writeToFile("graphDuplicates.txt", "Duplicate: ", e.getMessage());
				}
				
				if (verbose) {
					tasksDone ++;
					if (tasksDone % 100 == 0) {
						int tasksLeft = queue.size();
						System.out.println("[GraphThread] Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
					}
				}
			}
		} catch (InterruptedException e) {
			System.out.println("Graph thread interrupted!");
			System.exit(1);
		}
	}
	
	private void performAction(QueueTask task) throws IOException {
		switch (task.getType()) {
		case HistographTokens.Types.PIT:
			performPITAction(task);
			break;
		case HistographTokens.Types.RELATION:
			performRelationAction(task);
			break;
		default:
			throw new IOException("Unexpected type received.");
		}
	}
	
	private void performPITAction(QueueTask task) throws IOException {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			GraphMethods.addNode(db, params);
			break;
		case HistographTokens.Actions.UPDATE:
			GraphMethods.updateNode(db, params);
			break;
		case HistographTokens.Actions.DELETE:
			GraphMethods.deleteNode(db, params);
			break;
		default:
			throw new IOException("Unexpected task received.");
		}
	}
	
	private void performRelationAction(QueueTask task) throws IOException {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			Node[] nodes = GraphMethods.addRelation(db, engine, params);
			AtomicInferencer.inferAtomic(db, engine, params, nodes[0], nodes[1]);
			break;
		case HistographTokens.Actions.UPDATE:
			GraphMethods.updateRelation(db, engine, params);
			break;
		case HistographTokens.Actions.DELETE:
			GraphMethods.deleteRelation(db, engine, params);
			AtomicInferencer.removeInferredAtomic(db, engine, params);
			break;
		default:
			throw new IOException("Unexpected task received.");
		}
	}
	
	private void writeToFile(String fileName, String header, String message) {
		try {
			FileWriter fileOut = new FileWriter(fileName, true);
			fileOut.write(header + message + "\n");
			fileOut.close();
		} catch (Exception e) {
			System.out.println("Unable to write '" + message + "' to file '" + fileName + "'.");
		}	
	}
}