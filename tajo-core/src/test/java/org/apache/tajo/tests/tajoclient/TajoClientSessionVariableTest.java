package org.apache.tajo.tests.tajoclient;

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

	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private String sessionName;
	private Map<String, String> map;
	
	// Test environment
	private static TajoTestingCluster cluster;

	public TajoClientSessionVariableTest(String sessionName, String sessionValue) {
		this.sessionName = sessionName;
		map = new HashMap<>();
		map.put(sessionName, sessionValue);
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ "test_name", "test_value" },
			{ "test_name", "" },
			{ "", "test_value" },
			
			// Added after the improvement of the test suite
			{ "", "" }
		});
	}
	
	// Setup the test environment
	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		client = cluster.newTajoClient();
	}

	// Cleanup the test environment
	@AfterClass
	public static void tearDown() {
		client.close();
	}
	
	@Test
	public void updateAndUnsetSessionVariablesTest() {

		// Add new session variables to the client
		client.updateSessionVariables(map);

		// Assert that the new session variables exists
		assertTrue(client.existSessionVariable(sessionName));
		
		// Unset the added session variables
		client.unsetSessionVariables(Lists.newArrayList(sessionName));

		// Assert that the session variables doesn't exits
		assertFalse(client.existSessionVariable(sessionName));
	}
}
