package org.apache.tajo.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TryTest {

	private static TajoTestingCluster cluster;
	private static TajoConf conf;
	private static TajoClient client;
	private static Path testDir;

	private String databaseName;

	public TryTest(String databaseName) {
		this.databaseName = databaseName;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "test_database" }, 
		});
	}

	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		conf = cluster.getConfiguration();
		client = cluster.newTajoClient();
		testDir = CommonTestingUtil.getTestDir();
	}

	@AfterClass
	public static void tearDown() {
		client.close();
	}

	@After
	public void cleanUp() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {
		int current = client.getAllDatabaseNames().size();
		for (String str : client.getAllDatabaseNames()) {
			System.out.println("AAAAAA " + str);
		}
		if (current > 0) {
			for (String str : client.getAllDatabaseNames()) {
				if (!str.equals("information_schema")) {
					client.dropDatabase(str);
				}
			}
		}
	}

	@Test
	public void createDatabaseTest() throws DuplicateDatabaseException {
		System.out.println("\n*************** TEST ***************");
		int after = client.getAllDatabaseNames().size();

		client.createDatabase(databaseName);

		System.out.println("\n-------------- CREATE --------------");
		System.out.println("Created database: " + databaseName);

		int before = client.getAllDatabaseNames().size();

		System.out.println("\n-------------- RESULT --------------");
		System.out.println("After: " + after);
		System.out.println("Before: " + before);

		assertTrue(after < before);

		System.out.println("\n************************************\n");
	}

	/*
	 * @Test public void createDatabaseTest() throws DuplicateDatabaseException {
	 * int current = client.getAllDatabaseNames().size();
	 * 
	 * System.out.println("\n*************** TEST ***************");
	 * System.out.println("\n-------------- CREATE --------------"); String p =
	 * IdentifierUtil.normalizeIdentifier("database_"); for (int i = 0; i < 10; i++)
	 * { assertEquals(current+i, client.getAllDatabaseNames().size());
	 * 
	 * System.out.println("Created database: " + p + i);
	 * client.createDatabase(p+i);;
	 * 
	 * assertEquals(current+i+1, client.getAllDatabaseNames().size()); }
	 * 
	 * System.out.println("\n************************************\n"); }
	 */

	// @Test
	public void dropDatabaseTest() {

	}

}
