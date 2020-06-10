package com.mylp.sparkproject.conf;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ConfigurationManagerTest {
	
	@Test
	public void getPropertiesTest() {
		String testValue1 = ConfigurationManager.getProperty("testKey1");
		String testValue2 = ConfigurationManager.getProperty("testKey2");
		
		assertEquals(testValue1, "testValue1");
		assertEquals(testValue2, "testValue2");
	}

}
