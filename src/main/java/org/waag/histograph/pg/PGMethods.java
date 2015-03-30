package org.waag.histograph.pg;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class PGMethods {

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
	
	public static void createTable (Connection pg, String tableName, String... fields) throws IOException {
		if (fields.length % 2 != 0) throw new IOException ("All fields need a datatype parameter supplied as well.");
		if (fields.length < 2) throw new IOException ("At least one column needs to be specified.");
		
		String query = "create table " + tableName + "(";
		for (int i=0; i<fields.length; i += 2) {
			query += fields[i] + " " + fields[i+1] + ", ";
		}
		query += "id serial primary key);";
		
		int result;
		try {
			Statement st = pg.createStatement();
			result = st.executeUpdate(query);
		} catch (SQLException e) {
			throw new IOException("Could not create new table. ", e);
		}
			
		if (result != 0) {
			throw new IOException("Unexpected response received when creating new table: " + result);
		}
	}
	
	public static void addToTable (Connection pg, String tableName, String... fields) throws SQLException, IOException {
		String[] columns = getColumnNames(pg, tableName);
		int nColumns = columns.length - 1; // ID column omitted
		
		String qs = "?";
		for (int i=1; i<nColumns; i++) {
			qs += ", ?";
		}
		
		PreparedStatement stmt = null;
		String query = "insert into " + tableName + " (";
		
		if (fields.length == nColumns) {
			for (int i=0; i<nColumns; i++) {
				query += columns[i];
				if ((i+1)<nColumns) query += ", ";
			}

			stmt = pg.prepareStatement(query + ") values (" + qs + ");");			
			for (int i=0; i<fields.length; i++) {
				stmt.setString(i+1, fields[i]);
			}
		} else if (fields.length == nColumns * 2) {
			for (int i=0; i<fields.length; i+=2) {
				query += columns[i];
				if ((i+2) < fields.length) query += ", ";
			}
			
			stmt = pg.prepareStatement(query + ") values (" + qs + ");");
			for (int i=1; i<fields.length; i+=2) {
				stmt.setString((i+1)/2, fields[i]);
			}
		} else {
			throw new IOException("The number of fields should be equal to the number of columns in the database.");
		}
		
		int result;
		try {
			result = stmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Query was " + stmt.toString());
			throw new IOException("Could not add data to table.", e);
		}
			
		if (result != 1) { // Should affect one row
			System.out.println("Query was " + stmt.toString());
			throw new IOException("Unexpected response received when adding data to table: " + result);
		}
	}
	
	public static int getColumnCount (Connection pg, String tableName) throws SQLException, IOException {
		String columnCountQuery = "select count(*) from information_schema.columns where table_name='" + tableName + "'";
		Statement st = pg.createStatement();
		ResultSet rs = st.executeQuery(columnCountQuery);
		
		rs.next();
		return rs.getInt(1);
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
}