package com.mylp.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
	
	private static Properties properties = new Properties();
	
	/**
	 * 静态代码块
	 * 
	 * java中，每一个类第一次使用的时候，就会被java虚拟机（JVM）中的类加载器，去从磁盘上的.class文件中加载出来，然后为每一个类都会构建一个class对象，就代表了这个类
	 * 
	 * 每一个类在第一次加载的时候，都会进行自身的初始化，那么类初始化的时候，会执行哪些操作呢？
	 * 就由每个类内部的static构成的静态代码块决定，我们自己可以在类中开发静态代码块，
	 * 类第一次使用的时候，就会加载，加载的时候，就会初始化类，初始化类的时候就会执行静态代码块。
	 * 
	 * 因此，对于我们的配置管理组件，就在静态代码块中，编写读取配置文件的代码，
	 * 这样的话，第一次外界代码调用这个ConfigurationManager类的静态方法的时候，就会加载配置文件中的数据。
	 * 
	 * 而且，放在静态代码块中还有一个好处，就是类的初始化在整个JVM的生命周期内，有且仅有一次，也就是说，
	 * 配置文件只会加载一次，然后以后就是重复使用，效率比较高，不用反复加载多次。
	 */
	
	static {
		try {
			/**
			 * 通过一个"类名.class"的方式，就可以获取到这个类在JVM中对应的class对象
			 * 然后再通过这个class对象的getClassLoader()方法，就可以获取到当初加载这个类的JVM
			 * 中的类加载器（classLoader），然后通过调用classLoader的getResourceAsStream()这个方法
			 * 就可以用类加载器去加载类加载器路径中指定的文件，
			 * 最终可以获取到一个，针对指定文件的输入流（InputStream）
			 */
			InputStream inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
			
			// 调用propertis的load()方法，给它传入一个文件的InputStream输入流
			// 即可将文件中的符合key=value格式的配置项都加载到properties对象中
			// 加载过后，此时properties对象中就有了配置文件中所有的key-value对象，
			// 然后外界其实就可以通过Properties对象获取指定key对应的value
			properties.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取指定key对应的value
	 * 
	 * 外界代码第一次调用ConfigurationManager类的getProperty静态方法时，JVM内部会发现
	 * ConfigurationManager类还不在JVM的内存中
	 * 
	 * 此时JVM就会使用自己的classLoader（类加载器），去对应的类所在的磁盘文件（.class文件）中
	 * 去加载ConfigurationManager类，到JVM的内存中来，并根据类内部的信息去创建一个class对象，class对象
	 * 中就包含了类的元信息，包括类有哪些field（Properties properties）；有哪些方法（getProperty）
	 * 
	 * 加载ConfigurationManager类的时候，还会初始化这个类，那么此时就执行类的static静态代码块
	 * 此时我们编写的静态代码块中的代码，就会加载my.properties文件的内容，到properties对象中来。
	 * 
	 * 下一次外界代码在调用ConfigurationManager的getProperty方法时，就不会再次加载类，不会再次初始化类，
	 * 和执行静态代码块了，所以也印证了，我们上面所说的，类只会加载一次，配置文件也仅仅会加载一次。
	 * @param key
	 * @return
	 */
	public static String getProperty(String key) {
		return properties.getProperty(key);
	}
	
	/**
	 * 获取整形的配置项
	 * @param key
	 * @return
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0;
	}

	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public static Long getLong(String key) {
		String value = getProperty(key);
		
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0L;
	}
}
