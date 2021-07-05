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
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public TajoClientDatabaseTest(String databaseName, Class<? extends Exception> expectedException) {
		this.databaseName = databaseName;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "test_database", null },
			{ "", SQLSyntaxError.class }
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
		System.out.println("\n*************** TEST ***************");
		
		int after = client.getAllDatabaseNames().size();

		client.createDatabase(databaseName);

		System.out.println("\n-------------- CREATE --------------");
		System.out.println("Created database: " + databaseName);
		System.out.println("N. of databases: " + client.getAllDatabaseNames().size());
		
		assertTrue(client.existDatabase(databaseName));
		
		client.dropDatabase(databaseName);
		
		int before = client.getAllDatabaseNames().size();
		
		System.out.println("\n-------------- DROP --------------");
		System.out.println("Dropped database: " + databaseName);
		System.out.println("N. of databases: " + before);

		System.out.println("\n-------------- RESULT --------------");
		System.out.println("After: " + after);
		System.out.println("Before: " + before);

		assertFalse(client.existDatabase(databaseName));

		System.out.println("\n************************************\n");
	}
	
	@Test
	public void createAndDropDatabaseByQuery() throws TajoException {
		System.out.println("\n*************** TEST ***************");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Raised exception: " + expectedException.getName());
		}
		
		int after = client.getAllDatabaseNames().size();
		
		String sql1 = "create database " + databaseName;

		client.executeQueryAndGetResult(sql1);

		System.out.println("\n-------------- CREATE --------------");
		System.out.println("Created database: " + databaseName);
		System.out.println("N. of databases: " + client.getAllDatabaseNames().size());
		
		assertTrue(client.existDatabase(databaseName));
		
		String sql2 = "drop database " + databaseName;
		
		client.updateQuery(sql2);
		
		int before = client.getAllDatabaseNames().size();
		
		System.out.println("\n-------------- DROP --------------");
		System.out.println("Dropped database: " + databaseName);
		System.out.println("N. of databases: " + before);

		System.out.println("\n-------------- RESULT --------------");
		System.out.println("After: " + after);
		System.out.println("Before: " + before);

		assertFalse(client.existDatabase(databaseName));

		System.out.println("\n************************************\n");
	}
	
	@Test
	public void createDuplicateDatabaseTest() throws DuplicateDatabaseException {
		System.out.println("\n*************** TEST ***************");
		
		exceptionRule.expect(DuplicateDatabaseException.class);
		
		client.createDatabase(databaseName);

		System.out.println("\n------------- CREATE_1 -------------");
		System.out.println("Created database: " + databaseName);
		System.out.println("N. of databases: " + client.getAllDatabaseNames().size());

		client.createDatabase(databaseName);

		System.out.println("\n------------- CREATE_2 -------------");
		System.out.println("Database name: " + databaseName);
		System.out.println("N. of databases: " + client.getAllDatabaseNames().size());
		System.out.println("Can't create duplicate database");

		System.out.println("\n************************************\n");
	}
	
	@Test
	public void dropCurrentDatabaseTest() throws DuplicateDatabaseException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
		System.out.println("\n*************** TEST ***************");
		
		exceptionRule.expect(TajoInternalError.class);

		client.createDatabase(databaseName);
		client.selectDatabase(databaseName);

		System.out.println("\n-------------- CREATE --------------");
		System.out.println("Created database: " + databaseName);
		System.out.println("Selected database: " + client.getCurrentDatabase());
		
		client.dropDatabase(databaseName);

		System.out.println("\n-------------- DROP --------------");
		System.out.println("Current database: " + databaseName);
		System.out.println("Can't drop current database");

		System.out.println("\n************************************\n");
	}
}
