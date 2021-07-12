package org.apache.tajo.tests.tajoclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.SQLSyntaxError;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TajoClientDatabaseTest {

	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private String databaseName;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private static TajoTestingCluster cluster;
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public TajoClientDatabaseTest(String databaseName, Class<? extends Exception> expectedException) {
		this.databaseName = databaseName;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			// Minimal test suite
			{ "test_database", null },
			{ "", SQLSyntaxError.class }
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
	public void createAndDropDatabaseTest() throws DuplicateDatabaseException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

		// Create new database
		client.createDatabase(databaseName);
		
		// Assert that new database exists
		assertTrue(client.existDatabase(databaseName));
		
		// Drop created database
		client.dropDatabase(databaseName);
		
		// Assert that the database doesn't exists
		assertFalse(client.existDatabase(databaseName));
	}
	
	@Test
	public void createAndDropDatabaseByQueryTest() throws TajoException {

		// Set expected exception
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}

		// Create new database
		String sql1 = "create database " + databaseName;
		client.executeQueryAndGetResult(sql1);
		
		// Assert that new database exists
		assertTrue(client.existDatabase(databaseName));
		
		// Drop created database
		String sql2 = "drop database " + databaseName;
		client.updateQuery(sql2);

		// Assert that the database doesn't exists
		assertFalse(client.existDatabase(databaseName));
	}
	
	@Test
	public void createDuplicateDatabaseTest() throws DuplicateDatabaseException {

		// Set expected exception
		exceptionRule.expect(DuplicateDatabaseException.class);
		
		// Create new database
		client.createDatabase(databaseName);

		// Try to create a new database with the same name as the previous
		client.createDatabase(databaseName);
	}
	
	@Test
	public void dropCurrentDatabaseTest() throws DuplicateDatabaseException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

		// Set expected exception
		exceptionRule.expect(TajoInternalError.class);

		// Create new database
		client.createDatabase(databaseName);
		
		// Select created database
		client.selectDatabase(databaseName);

		// Try to drop selected database
		client.dropDatabase(databaseName);
	}
}
