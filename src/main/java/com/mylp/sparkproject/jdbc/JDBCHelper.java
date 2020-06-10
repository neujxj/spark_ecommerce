package com.mylp.sparkproject.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import com.mylp.sparkproject.conf.ConfigurationManager;
import com.mylp.sparkproject.constant.Constants;

/**
 * JDBC 辅助组件
 * 
 * 
 * 
 * @author Administrator
 *
 */
public class JDBCHelper {
	
	// 在静态代码中加载mysql驱动
	static {
		try {
			Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static JDBCHelper instance = null;
	
	public static JDBCHelper getInstance() {
		if (instance == null) {
			synchronized (JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}				
			}
		}
		return instance;
	}
	
	private LinkedList<Connection> datasource = new LinkedList<Connection>();
	
	private JDBCHelper() {
		int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
		
		for (int i = 0; i < datasourceSize; i++) {
			try {
				Connection connection = DriverManager.getConnection(
						ConfigurationManager.getProperty(Constants.JDBC_URL),
						ConfigurationManager.getProperty(Constants.JDBC_USER),
						ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
					); 
				datasource.push(connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public synchronized Connection getConnection() {
		while (datasource.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return datasource.poll();
	}
	
	public int executeUpdate(String sql, Object[] params) {
		int ret = 0;
		Connection connection = null;
		PreparedStatement pstmt = null;
		
		try {
			connection = getConnection();
			pstmt = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				pstmt.setObject(i+1, params[i]);
			}
			ret = pstmt.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				datasource.push(connection);
			}
		}
		
		return ret;
	}
	
	public void executeQuery(String sql, Object[] params, QueryCallback callback) {
		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			connection = getConnection();
			pstmt = connection.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				pstmt.setObject(i+1, params[i]);
			}
			rs = pstmt.executeQuery();
			callback.process(rs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				datasource.push(connection);
			}
		}
	}
	
	/**
	 * 批量执行sql语句
	 * 
	 * 批量执行sql语句是JDBC中的高级功能
	 * 默认情况下，每次执行一条SQL语句，就会通过网络连接，给mysql发送一次请求，
	 * 
	 * 但是，如果短时间内要执行多条结构一模一样的sql，只是参数不同，
	 * 虽然使用PreparedStatement这种方式，可以只编译一次sql，提高性能，但是，还是对于每次sql
	 * 都要向mysql发送一次网络请求
	 * 
	 * 可以通过批量执行sql语句的功能优化这个性能
	 * 一次性通过PreparedStatement发送多条sql语句，比如100、1000条，执行的时候也只编译一次就可以
	 * 这种批量执行sql语句的方式，可以大大提升性能。
	 * @param sql
	 * @param paramList
	 * @return
	 */
	public int[] executeBatch(String sql, List<Object[]> paramList) {
		int[] rets = null;
		Connection connection = null;
		PreparedStatement pstmt = null;
		
		try {
			connection = getConnection();
			connection.setAutoCommit(false);
			
			pstmt = connection.prepareStatement(sql);
			
			for(Object[] params : paramList) {
				for (int i = 0; i < params.length; i++) {
					pstmt.setObject(i+1, params[i]);
				}
				pstmt.addBatch();
			}
			
			rets = pstmt.executeBatch();
			connection.commit();
			// 重新设置为自动提交模式
			connection.setAutoCommit(true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				datasource.push(connection);
			}
		}
		
		return rets;
	}
	
	public static interface QueryCallback {
		
		// 处理查询结果
		void process(ResultSet rs) throws Exception;
	}
}
