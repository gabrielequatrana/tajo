package org.apache.tajo.tests.tajoclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.SQLSyntaxError;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.exception.UndefinedTableException;
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
public class TajoClientTableTest {
	
	// TajoClient instance
	private static TajoClient client;
	
	// Test parameters
	private String tableName;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private static TajoTestingCluster cluster;
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public TajoClientTableTest(String tableName, Class<? extends Exception> expectedException) {
		this.tableName = tableName;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "test_table", null }, 
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
	public void cleanUp() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, UndefinedTableException {
		int tables = client.getTableList(null).size();
		if (tables > 0) {
			for (String str : client.getTableList(null)) {
				client.dropTable(str);
			}
		}
	}
	
	@Test
	public void createAndDropTableByQueryTest() throws TajoException, IOException {
		System.out.println("\n*************** TEST ***************");	
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Raised exception: " + expectedException.getName());
		}
		
		int after = client.getTableList(null).size();

		String sql1 = "create table " + tableName + " (deptname text, score int4)";
		
		client.updateQuery(sql1);
		
		System.out.println("\n-------------- CREATE --------------");
		System.out.println("Created table: " + tableName);
		System.out.println("N. of tables: " + client.getTableList(null).size());
		
		assertTrue(client.existTable(tableName));

		String sql2 = "drop table " + tableName;
		
		client.updateQuery(sql2);
		
		int before = client.getTableList(null).size();
		
		System.out.println("\n-------------- DROP --------------");
		System.out.println("Dropped table: " + tableName);
		System.out.println("N. of tables: " + before);
	
		System.out.println("\n-------------- RESULT --------------");
		System.out.println("After: " + after);
		System.out.println("Before: " + before);
		
		assertFalse(client.existTable(tableName));
		
		System.out.println("\n************************************\n");
	}
	
	@Test
	public void createAndPurgeTableByQueryTest() throws TajoException, IOException {
		System.out.println("\n*************** TEST ***************");	
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Raised exception: " + expectedException.getName());
		}
		
		int after = client.getTableList(null).size();

		String sql1 = "create table " + tableName + " (deptname text, score int4)";
		
		client.updateQuery(sql1);
		
		System.out.println("\n-------------- CREATE --------------");
		System.out.println("Created table: " + tableName);
		System.out.println("N. of tables: " + client.getTableList(null).size());
		
		assertTrue(client.existTable(tableName));

		String sql2 = "drop table " + tableName + " purge";
		
		client.updateQuery(sql2);
		
		int before = client.getTableList(null).size();
		
		System.out.println("\n-------------- DROP --------------");
		System.out.println("Dropped table: " + tableName);
		System.out.println("N. of tables: " + before);
	
		System.out.println("\n-------------- RESULT --------------");
		System.out.println("After: " + after);
		System.out.println("Before: " + before);
		
		assertFalse(client.existTable(tableName));
		
		System.out.println("\n************************************\n");
	}
}
