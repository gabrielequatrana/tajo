package org.apache.tajo.tests.catalogadminclient;

import static org.junit.Assert.assertFalse;
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
public class CatalogAdminClientDropDatabaseTest {

	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private String databaseName;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private static TajoTestingCluster cluster;
	private boolean dropCurrent;
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public CatalogAdminClientDropDatabaseTest(String databaseName, boolean dropCurrent, Class<? extends Exception> expectedException) {
		this.databaseName = databaseName;
		this.dropCurrent = dropCurrent;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ "test_database", false, null },
			{ "database", false, UndefinedDatabaseException.class },
			
			// Added after the improvement of the test suite
			{ "test_database", true, CannotDropCurrentDatabaseException.class },
		});
	}

	// Setup the test environment
	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		client = cluster.newTajoClient();
	}
	
	@Before
	public void configuration() throws DuplicateDatabaseException {
		client.createDatabase("test_database");
		
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
	public void dropDatabaseTest() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

		if (dropCurrent) {

			// Select created database
			client.selectDatabase(databaseName);
		}
		
		// Drop created database
		client.dropDatabase(databaseName);
		
		// Assert that the database doesn't exists
		assertFalse(client.existDatabase(databaseName));
	}
}
