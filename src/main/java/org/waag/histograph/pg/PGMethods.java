package org.waag.histograph.pg;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class 	PGMethods {

	public static Set<String> getTableNames (Connection pg) throws SQLException {
		Set<String> out = new HashSet<String>();

		Statement st = pg.createStatement();
		ResultSet rs = st.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';");
		while (rs.next()) {
			out.add(rs.getString(1));
		}
		rs.close();
		st.close();
		
		return out;
	}
	
	public static boolean tableExists (Connection pg, String tableName) throws SQLException, IOException {
		Statement st = pg.createStatement();
		ResultSet rs = st.executeQuery("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + tableName + "');");
		
		rs.next();
		if (rs.getString(1).equals("f")) return false;
		if (rs.getString(1).equals("t")) return true;
		
		throw new IOException("Unexpected result retrieved: " + rs.getString(1));
	}
	
	public static void createTable (Connection pg, String tableName, String... fields) throws SQLException, IOException {
		if (fields.length % 2 != 0) throw new IOException ("All fields need a datatype parameter supplied as well.");
		if (fields.length < 2) throw new IOException ("At least one column needs to be specified.");
		
		String query = "create table " + tableName + "(";
		for (int i=0; i<fields.length; i += 2) {
			query += fields[i] + " " + fields[i+1];
			if ((i+2)<fields.length) query += ", ";
		}
		query += ");";
		
		int result;
		Statement st = null;
		
		try {
			st = pg.createStatement();
			result = st.executeUpdate(query);
		} catch (SQLException e) {
			throw new IOException("Could not create new table. ", e);
		} finally {
			if (st != null) st.close();
		}
			
		if (result != 0) {
			throw new IOException("Unexpected response received when creating new table: " + result);
		}
	}
	
	public static void deleteFromTable (Connection pg, String tableName, String... keyVals) throws SQLException, IOException {
		if (keyVals.length % 2 != 0) throw new IOException("getRowsByKeyValPairs expects an even amount of parameters.");
		if (keyVals.length < 2) throw new IOException("getRowsByKeyValPairs expects at least two parameters.");

		String[] columns = getColumnNames(pg, tableName);

		for (int i=0; i<keyVals.length; i+=2) {
			boolean columnFound = false;
			for (String col : columns) {
				if (col.equals(keyVals[i])) columnFound = true;
			}
			if (!columnFound) throw new IOException("Column '" + keyVals[i] + "' not found in table '" + tableName + "'.");
		}

		String query = "DELETE FROM " + tableName + " WHERE ";
		
		for (int i=0; i<keyVals.length; i+=2) {
			query += keyVals[i] + " = ?";
			if ((i+2)<keyVals.length) query += " AND ";
		}
		query += ";";
		
		PreparedStatement stmt = pg.prepareStatement(query);
		
		for (int i=1; i<keyVals.length; i+=2) {
			stmt.setString((i+1)/2, keyVals[i]);
		}
		
		stmt.executeUpdate();
		
		int result;
		try {
			result = stmt.executeUpdate();
		} catch (SQLException e) {
			throw new IOException("Could not remove data from table.", e);
		} finally {
			stmt.close();
		}
			
		if (result != 0) { // Should return 0
			throw new IOException("Unexpected response received when adding data to table: " + result);
		}
	}
	
	public static void addToTable (Connection pg, String tableName, String... keyVals) throws SQLException, IOException {
		String[] columns = getColumnNames(pg, tableName);
		int nColumns = columns.length;
		
		String qs = "?";
		for (int i=1; i<nColumns; i++) {
			qs += ", ?";
		}
		
		PreparedStatement stmt = null;
		String query = "insert into " + tableName + " (";
		
		if (keyVals.length == nColumns) {
			for (int i=0; i<nColumns; i++) {
				query += columns[i];
				if ((i+1)<nColumns) query += ", ";
			}

			stmt = pg.prepareStatement(query + ") values (" + qs + ");");			
			for (int i=0; i<keyVals.length; i++) {
				stmt.setString(i+1, keyVals[i]);
			}
		} else if (keyVals.length == nColumns * 2) {
			for (int i=0; i<keyVals.length; i+=2) {
				query += keyVals[i];
				if ((i+2) < keyVals.length) query += ", ";
			}
			
			stmt = pg.prepareStatement(query + ") values (" + qs + ");");
			for (int i=1; i<keyVals.length; i+=2) {
				stmt.setString((i+1)/2, keyVals[i]);
			}
		} else {
			throw new IOException("The number of fields should be equal to the number of columns in the database.");
		}
		
		int result;
		try {
			result = stmt.executeUpdate();
		} catch (SQLException e) {
			throw new IOException("Could not add data to table.", e);
		} finally {
			stmt.close();
		}
			
		if (result != 1) { // Should affect one row
			throw new IOException("Unexpected response received when adding data to table: " + result);
		}
	}
	
	public static int getColumnCount (Connection pg, String tableName) throws SQLException, IOException {
		String columnCountQuery = "select count(*) from information_schema.columns where table_name='" + tableName + "'";
		Statement st = pg.createStatement();
		ResultSet rs = st.executeQuery(columnCountQuery);
		
		rs.next();
		int out = rs.getInt(1);
		
		rs.close();
		st.close();
		
		return out;
	}
	
	public static String[] getColumnNames (Connection pg, String tableName) throws SQLException, IOException {
		String columnQuery = "SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + tableName + "'";
		Statement st = pg.createStatement();
		ResultSet rs = st.executeQuery(columnQuery);
		ArrayList<String> columns = new ArrayList<String>();
		
		while (rs.next()) {
			columns.add(rs.getString("column_name"));
		}
		rs.close();
		st.close();
		
		String[] out = new String[columns.size()];
		out = columns.toArray(out);
		return out;
	}
	
	public static boolean rowExists(Connection pg, String tableName, String... keyVals) throws IOException, SQLException {
		String[] columns = getColumnNames(pg, tableName);
		int nColumns = columns.length;
		
		PreparedStatement stmt = null;
		String query = "select count(*) from " + tableName + " where ";

		if (keyVals.length == nColumns) {
			for (int i=0; i<nColumns; i++) {
				query += columns[i] + " = ? ";
				if ((i+1)<nColumns) query += " and ";
			}
			query += ";";

			stmt = pg.prepareStatement(query);			
			for (int i=0; i<keyVals.length; i++) {
				stmt.setString(i+1, keyVals[i]);
			}
		} else if (keyVals.length == nColumns * 2) {
			for (int i=0; i<keyVals.length; i+=2) {
				query += keyVals[i] + " = ? ";
				if ((i+2) < keyVals.length) query += " and ";
			}
			query += ";";
			
			stmt = pg.prepareStatement(query);
			for (int i=1; i<keyVals.length; i+=2) {
				stmt.setString((i+1)/2, keyVals[i]);
			}
		} else {
			throw new IOException("The number of fields should be equal to the number of columns in the database.");
		}
		
		ResultSet rs = stmt.executeQuery();

		try {
			rs.next();
			int out = rs.getInt(1);
			return (out > 0);
		} catch (SQLException e) {
			System.out.println("Query was " + stmt.toString());
			throw new IOException("Could not check if row exists.", e);
		} finally {
			stmt.close();
		}
	}
	
	public static Map<String, String>[] getRowsByKeyValPairs (Connection pg, String tableName, String... keyVals) throws SQLException, IOException {
		if (keyVals.length % 2 != 0) throw new IOException("getRowsByKeyValPairs expects an even amount of parameters.");
		if (keyVals.length < 2) throw new IOException("getRowsByKeyValPairs expects at least two parameters.");

		String[] columns = getColumnNames(pg, tableName);

		for (int i=0; i<keyVals.length; i+=2) {
			boolean columnFound = false;
			for (String col : columns) {
				if (col.equals(keyVals[i])) columnFound = true;
			}
			if (!columnFound) throw new IOException("Column '" + keyVals[i] + "' not found in table '" + tableName + "'.");
		}

		String query = "SELECT * FROM " + tableName + " WHERE ";
		
		for (int i=0; i<keyVals.length; i+=2) {
			query += keyVals[i] + " = ?";
			if ((i+2)<keyVals.length) query += " AND ";
		}
		query += ";";
			
		ArrayList<Map<String, String>> results = new ArrayList<Map<String, String>>();
		
		PreparedStatement stmt = pg.prepareStatement(query);
		
		for (int i=1; i<keyVals.length; i+=2) {
			stmt.setString((i+1)/2, keyVals[i]);
		}
		
		ResultSet rs = stmt.executeQuery();
		
		while (rs.next()) {
			Map<String, String> result = new HashMap<String, String>();
			for (String col : columns) {
				result.put(col, rs.getString(col));
			}
			results.add(result);
		}
		
		rs.close();
		stmt.close();
		
		if (results.isEmpty()) return null;
		
		@SuppressWarnings("unchecked")
		Map<String, String>[] out = new Map[results.size()];
		out = results.toArray(out);
		
		return out;
	}
	
	public static Map<String, String>[] getAllRows (Connection pg, String tableName) throws SQLException, IOException {
		String query = "SELECT * FROM " + tableName + ";";
		String[] columns = getColumnNames(pg, tableName);
		
		ArrayList<Map<String, String>> results = new ArrayList<Map<String, String>>();
		
		PreparedStatement stmt = pg.prepareStatement(query);		
		ResultSet rs = stmt.executeQuery();
		
		while (rs.next()) {
			Map<String, String> result = new HashMap<String, String>();
			for (String col : columns) {
				result.put(col, rs.getString(col));
			}
			results.add(result);
		}
		
		rs.close();
		stmt.close();
		
		if (results.isEmpty()) return null;
		
		@SuppressWarnings("unchecked")
		Map<String, String>[] out = new Map[results.size()];
		out = results.toArray(out);
		
		return out;
	}
	
	public static boolean indexExists (Connection pg, String tableName, String column) {
		String indexName = tableName + "_" + column + "_idx";
		String query = "SELECT 'public." + indexName + "'::regclass";
		
		try {
			Statement st = pg.createStatement();
			st.executeQuery(query);
		} catch (SQLException e) {
			return false;
		}
		return true;		
	}
	
	public static void createIndex (Connection pg, String tableName, String column) throws IOException, SQLException {
		String query = "CREATE INDEX " + tableName + "_" + column + "_idx ON " + tableName + "(" + column + ")";
		
		int result;
		Statement st = null;
		
		try {
			st = pg.createStatement();
			result = st.executeUpdate(query);
		} catch (SQLException e) {
			throw new IOException("Could not create index. ", e);
		} finally {
			if (st != null) st.close();
		}
			
		if (result != 0) {
			throw new IOException("Unexpected response received when creating index: " + result);
		}
	}
}