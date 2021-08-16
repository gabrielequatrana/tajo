package org.apache.tajo.tests.catalogadminclient;

import static org.junit.Assert.assertFalse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.DuplicateTableException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UnavailableTableLocationException;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.tests.util.BackendTestingUtil;
import org.apache.tajo.tests.util.CatalogAdminClientTestUtil;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.apache.tajo.util.CommonTestingUtil;
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
public class CatalogAdminClientDropTableTest {

	// TajoClient instance
	private static TajoClient client;

	// Test parameters
	private String tableName;
	private boolean purge;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private static TajoTestingCluster cluster;
	private static TajoConf conf;
	private static Path testDir;

	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public CatalogAdminClientDropTableTest(String tableName, boolean purge, Class<? extends Exception> expectedException) throws IOException {
		this.tableName = tableName;
		this.purge = purge;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ "test_table", false, null }, 
			{ "test_table", true, null },
			{ "table", false, UndefinedTableException.class },
		});
	}

	// Setup the test environment
	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		conf = cluster.getConfiguration();
		testDir = CommonTestingUtil.getTestDir();
		client = cluster.newTajoClient();
	}
	
	@Before
	public void configuration() throws DuplicateTableException, UnavailableTableLocationException, InsufficientPrivilegeException, IOException {
		Path path = CatalogAdminClientTestUtil.createTempTable(tableName, testDir, conf);
		client.createExternalTable("test_table", BackendTestingUtil.mockupSchema, path.toUri(), BackendTestingUtil.mockupMeta);
		
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
	public void cleanUp() throws InsufficientPrivilegeException, UndefinedTableException {
		int tables = client.getTableList(null).size();
		if (tables > 0) {
			for (String str : client.getTableList(null)) {
				client.dropTable(str);
			}
		}
	}

	@Test
	public void dropTableTest() throws UndefinedTableException, InsufficientPrivilegeException {
		
		// Drop table
		client.dropTable(tableName, purge);

		// Assert that the table doesn't exists
		assertFalse(client.existTable(tableName));
	}
}
