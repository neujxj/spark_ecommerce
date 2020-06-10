package com.mylp.sparkproject.conf;

import com.mylp.sparkproject.conf.School.Teacher;

import scala.annotation.meta.companionClass;
import shapeless.newtype;

interface ISayHello {
	String sayHello(String name);
}

//class School {
//	private String name;
//	
//	static class Teacher {
//		private String name;
//
//		public String getName() {
//			return name;
//		}
//
//		public void setName(String name) {
//			this.name = name;
//		}
//	}
//
//	public String getName() {
//		return name;
//	}
//
//	public void setName(String name) {
//		this.name = name;
//	}
//}

class School {
	private String name;
	
	class Teacher {
		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}

public class InnerClassTest {
	
	public static void main(String args[]) {
		ISayHello sayHello = new ISayHello() {

			@Override
			public String sayHello(String name) {
				return "hello " + name;
			}
			
		};
		
		System.out.println(sayHello.sayHello("leo"));
		
//		Teacher teacher = new Teacher();
//		teacher.setName("Tom");
//		System.out.println(teacher.getName());
		
		School school = new School();
		
		
		School.Teacher teacher = school.new Teacher();
	}
	

}
