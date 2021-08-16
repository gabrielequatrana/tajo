package org.apache.tajo.tests.sessionconnection;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.apache.tajo.tests.util.SessionConnectionTestUtil;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.internal.util.reflection.FieldSetter;

@RunWith(Parameterized.class)
public class SessionConnectionGetSessionVariableTest {

	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private String varname;
	private boolean improvement;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private static TajoTestingCluster cluster;
	private static String key = "test_key";
	private static String expectedValue = "test_value";
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public SessionConnectionGetSessionVariableTest(String varname, boolean improvement, Class<? extends Exception> expectedException) {
		this.varname = varname;
		this.improvement = improvement;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ key, false, null },
			{ "key", false, NoSuchSessionVariableException.class },
			
			// Added after the improvement of the test suite
			{ key, true, null }
		});
	}

	// Setup the test environment
	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		client = cluster.newTajoClient();
		client.updateSessionVariables(SessionConnectionTestUtil.getVars(key, expectedValue));
	}
	
	@Before
	public void configuration() {
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
	}

	// Cleanup the test environment
	@AfterClass
	public static void tearDown() {
		client.close();
	}
	
	@Test
	public void updateSessionVariablesTest() throws NoSuchSessionVariableException, Exception {
		
		if (improvement) {
			
			// Clear client side cache
			FieldSetter.setField(client, SessionConnection.class.getDeclaredField("sessionVarsCache"), new HashMap<String, String>());
		}
		
		// Retrieve the variable value
		String actualValue = client.getSessionVariable(varname);
		
		// Assert that retrieved value is equal to the added value
		assertEquals(expectedValue, actualValue);
	}
	
	public void reflect() throws IllegalArgumentException, IllegalAccessException {
		Class<?> secretClass = client.getClass();
		Field fields[] = secretClass.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			System.out.println(field.get(client) + "\n");
		}
	}
}
