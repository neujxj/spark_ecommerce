package com.mylp.sparkproject.common;

import org.junit.Test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class FastJsonTest {
	
	@Test
	public void fastjsonTest() {
		String json = "[{'姓名':'张三', '班级':'一班', '科目':'高数', '分数':90}, {'姓名':'李四', '班级':'一班', '科目':'高数', '分数':80}]";
		
		JSONArray jsonArray = JSONArray.parseArray(json);
		JSONObject jsonObject = jsonArray.getJSONObject(0);
		System.out.println("姓名：" + jsonObject.getString("姓名"));
	}
}
