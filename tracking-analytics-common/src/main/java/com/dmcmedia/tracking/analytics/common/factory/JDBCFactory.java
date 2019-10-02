package com.dmcmedia.tracking.analytics.common.factory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

public class JDBCFactory {
	
	public static void jdbcClientDelete(String tb, String whereStmt, HashMap<Integer, Object> whereParams) {
		Connection con = null;
		PreparedStatement pstmt = null;
		
		String sql = "delete from " + tb + " where " + whereStmt;
		
		try {
			//연결
			con = DriverManager.getConnection(
					SparkJdbcConnectionFactory.getDefaultJdbcUrl()
					, SparkJdbcConnectionFactory.getJdbcUser()
					, SparkJdbcConnectionFactory.getJdbcPassword());
			//구문
			pstmt = con.prepareStatement(sql);
			
			for(int key : whereParams.keySet()) {
				if (whereParams.get(key) instanceof String) {
					pstmt.setString(key, (String) whereParams.get(key));
				} else if (whereParams.get(key) instanceof Integer) {
					pstmt.setInt(key, (int) whereParams.get(key));
				}
			}
//			System.out.println(">>>> pstmt = " + pstmt);
			
			int r = pstmt.executeUpdate();
//			System.out.println("deleted row count = " + r);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e2) {
					e2.printStackTrace();
				}
			}
			
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e3) {
					e3.printStackTrace();
				}
			}
		}
	}
	
}
