package org.apache.tajo.tests.sessionconnection;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.tests.util.SessionConnectionTestUtil;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SessionConnectionUpdateSessionVariablesTest {

	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private Map<String, String> variables;
	
	// Test environment
	private static TajoTestingCluster cluster;
	private static String key = "test_key";
	private static String value = "test_value";
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public SessionConnectionUpdateSessionVariablesTest(Map<String, String> variables) {
		this.variables = variables;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ SessionConnectionTestUtil.getVars(key, value) },
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
	public void updateSessionVariablesTest() {
		
		// Update session variables
		client.updateSessionVariables(variables);
		
		// Retrieve variable key
		List<String> keys = new ArrayList<String>(variables.keySet());
		String key = keys.get(0);
		
		// Assert that the variable exists
		assertTrue(client.existSessionVariable(key));
	}
}
