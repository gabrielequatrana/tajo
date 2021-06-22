package org.apache.tajo.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import jersey.repackaged.com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class TajoClientSessionVariableTest {

	private static TajoTestingCluster cluster;
	private static TajoClient client;
	
	private String sessionName;
	private String sessionValue;
	private Map<String, String> map;

	public TajoClientSessionVariableTest(String sessionName, String sessionValue) {
		this.sessionName = sessionName;
		this.sessionValue = sessionValue;
		map = new HashMap<>();
		map.put(sessionName, sessionValue);
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "test_name", "test_value" },
			{ "test_name", "" },
			{ "", "test_value" },
			{ "", "" }
		});
	}
	
	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		client = cluster.newTajoClient();
	}

	@AfterClass
	public static void tearDown() {
		client.close();
	}
	
	@Test
	public void updateAndUnsetSessionVariablesTest() {
		System.out.println("\n*************** TEST ***************");
		
		int after = client.getAllSessionVariables().size();

		client.updateSessionVariables(map);

		System.out.println("\n-------------- UPDATE --------------");
		System.out.println("Updated varibale: " + sessionName + ":" + sessionValue);
		System.out.println("N. of variables: " + client.getAllSessionVariables().size());
		
		assertTrue(client.existSessionVariable(sessionName));
		
		client.unsetSessionVariables(Lists.newArrayList(sessionName));
		
		int before = client.getAllDatabaseNames().size();
		
		System.out.println("\n-------------- UNSET --------------");
		System.out.println("Unsetted varibale: " + sessionName + ":" + sessionValue);
		System.out.println("N. of varibales: " + before);

		System.out.println("\n-------------- RESULT --------------");
		System.out.println("After: " + after);
		System.out.println("Before: " + before);

		assertFalse(client.existSessionVariable(sessionName));

		System.out.println("\n************************************\n");	
	}
}
