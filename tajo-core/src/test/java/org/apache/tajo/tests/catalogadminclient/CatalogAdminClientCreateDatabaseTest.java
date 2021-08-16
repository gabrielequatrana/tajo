package org.apache.tajo.tests.catalogadminclient;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
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
public class CatalogAdminClientCreateDatabaseTest {

	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private String databaseName;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private static TajoTestingCluster cluster;
	private boolean duplicate;
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public CatalogAdminClientCreateDatabaseTest(String databaseName, boolean duplicate, Class<? extends Exception> expectedException) {
		this.databaseName = databaseName;
		this.duplicate = duplicate;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ "test_database", false, null },
			
			// Added after the improvement of the test suite
			{ "test_database", true, DuplicateDatabaseException.class },
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
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
	}

	// Cleanup the test environment
	@AfterClass
	public static void tearDown() {
		client.close();
	}

	@After
	public void cleanUp() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
		client.selectDatabase("default");
		
		int databases = client.getAllDatabaseNames().size();
		if (databases > 2) {
			for (String str : client.getAllDatabaseNames()) {
				if (!str.equals("information_schema") && !str.equals("default")) {
					client.dropDatabase(str);
				}
			}
		}
	}
	
	@Test
	public void createDatabaseTest() throws DuplicateDatabaseException  {
		
		// Create new database
		client.createDatabase(databaseName);
		
		if (duplicate) {

			// Try to create a database with the same name
			client.createDatabase(databaseName);
		}
		
		// Assert that new database exists
		assertTrue(client.existDatabase(databaseName));
	}
}
