package org.apache.tajo.tests.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionConnectionTestUtil {

	private SessionConnectionTestUtil() {
		
	}
	
	public static Map<String, String> getVars(String key, String value) {
		Map<String, String> validVar = new HashMap<>();
		validVar.put(key, value);
		return validVar;
	}
	
	public static List<String> getKeys(String key) {
		List<String> var = new ArrayList<>();
		var.add(key);
		return var;
	}
}
