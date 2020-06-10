package com.mylp.sparkproject.jdbc;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.mylp.sparkproject.jdbc.JDBCHelper.QueryCallback;

public class JDBCHelperTest {
	
	@Test
	public void executeUpdate() {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		jdbcHelper.executeUpdate("insert into test_user(name, age) values(?,?)", new Object[] {"张三", 28});
	}
	
	@Test
	public void executeBatch() {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		String sql = "insert into test_user(name, age) values(?,?)";
		
		List<Object[]> paramList = new ArrayList<Object[]>();
		paramList.add(new Object[] {"李四", 29});
		paramList.add(new Object[] {"王五", 30});
		
		jdbcHelper.executeBatch(sql, paramList);
	}
	
	@Test
	public void executeQuery() {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		final Map<String, Object> testUser = new HashMap<String, Object>();
		
		jdbcHelper.executeQuery("select name, age from test_user where id = ?", 
				new Object[] {1}, new QueryCallback() {
					
					@Override
					public void process(ResultSet rs) throws Exception {
						if (rs.next()) {
							String name = rs.getString("name");
							int age = rs.getInt("age");
							
							testUser.put("name", name);
							testUser.put("age", age);
						}
					}
				});
		
		System.out.println("name: " + testUser.get("name") + ", age: " + testUser.get("age"));
	}

}
