package org.apache.tajo.tests.sessionconnection;

import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.tests.util.SessionConnectionTestUtil;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SessionConnectionUnsetSessionVariablesTest {

	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private List<String> variables;
	
	// Test environment
	private static TajoTestingCluster cluster;
	private static String key = "test_key";
	private static String value = "test_value";
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public SessionConnectionUnsetSessionVariablesTest(List<String> variables) {
		this.variables = variables;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ SessionConnectionTestUtil.getKeys(key) },
			{ SessionConnectionTestUtil.getKeys("") },
		});
	}

	// Setup the test environment
	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		client = cluster.newTajoClient();
	}
	
	@Before
	public void configuration() {
		client.updateSessionVariables(SessionConnectionTestUtil.getVars(key, value));
	}

	// Cleanup the test environment
	@AfterClass
	public static void tearDown() {
		client.close();
	}

	@After
	public void cleanUp() {
		if (client.existSessionVariable(key)) {
			client.unsetSessionVariables(SessionConnectionTestUtil.getKeys(key));
		}
	}
	
	@Test
	public void unsetSessionVariablesTest() {
		
		client.unsetSessionVariables(variables);
		
		String key = variables.get(0);
		
		// Assert that new database exists
		assertFalse(client.existSessionVariable(key));
	}
}
