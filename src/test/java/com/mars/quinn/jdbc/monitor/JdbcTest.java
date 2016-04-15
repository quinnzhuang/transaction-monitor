package com.mars.quinn.jdbc.monitor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;
import org.testng.Assert;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class JdbcTest {

	public final static String DB_DRIVER_CLASS = "com.mysql.jdbc.Driver";
	public final static String DB_URL = "jdbc:mysql://localhost:3306/oasis";
	public final static String DB_USERNAME = "root";
	public final static String DB_PASSWORD = "root";

	public static DataSource getMySQLDataSource() {
		MysqlDataSource mysqlDS = null;
		mysqlDS = new MysqlDataSource();
		mysqlDS.setURL(DB_URL);
		mysqlDS.setUser(DB_USERNAME);
		mysqlDS.setPassword(DB_PASSWORD);
		return new DataSourceMonitor(mysqlDS);
	}

	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		Connection con = null;
		Class.forName(DB_DRIVER_CLASS);
		con = getMySQLDataSource().getConnection();
		System.out.println("DB Connection created successfully");
		return con;
	}

	@Test
	public void test() {
		Connection con = null;
		try {
			con = getConnection();
			// set auto commit to false
			con.setAutoCommit(false);
			insertTest(con, 3, 3);
			insertTest(con, 2, 2);
			// now commit transaction
			con.commit();
			Assert.assertEquals(false, con.getAutoCommit());
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				con.rollback();
				System.out.println("JDBC Transaction rolled back successfully");
			} catch (SQLException e1) {
				System.out.println("SQLException in rollback" + e.getMessage());
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void insertTest(Connection connection, int a, int b) throws SQLException {
		PreparedStatement statement = connection.prepareStatement("insert into test(a, b) values(?, ?)");
		statement.setInt(1, a);
		statement.setInt(2, b);
		statement.execute();
		statement.close();
	}

}
