package org.apache.tajo.tests.tajoclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.DuplicateTableException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.SQLSyntaxError;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UnavailableTableLocationException;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.tests.util.BackendTestingUtil;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.apache.tajo.util.CommonTestingUtil;
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
public class TajoClientExternalTableTest {

	// TajoClient instance
	private static TajoClient client;

	// Test parameters
	private String tableName;
	private Path path;
	private Class<? extends Exception> expectedException;

	// Test environment
	private static TajoTestingCluster cluster;
	private static TajoConf conf;
	private static Path testDir;

	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public TajoClientExternalTableTest(String tableName, String path, Class<? extends Exception> expectedException) throws IOException {
		this.tableName = tableName;
		this.path = createTempTable(path);
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			// Minimal test suite
			{ "test_table", "test_table", null }, 
			{ "", "test_table", null },
			{ "test_table", "", SQLSyntaxError.class }
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
	public void createAndDropExternalTableTest() throws UndefinedTableException, InsufficientPrivilegeException, DuplicateTableException, UnavailableTableLocationException {
		
		// Create new external table
		client.createExternalTable(tableName, BackendTestingUtil.mockupSchema, path.toUri(), BackendTestingUtil.mockupMeta);

		// Assert that new table exists
		assertTrue(client.existTable(tableName));

		// Drop created table
		client.dropTable(tableName);

		// Assert that the table doesn't exists
		assertFalse(client.existTable(tableName));
	}

	@Test
	public void createAndDropExternalTableByQueryTest() throws TajoException {

		// Set expected exception
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}

		// Create new external table
		String sql1 = "create external table " + tableName + " (deptname text, score int4) " + "using csv location '" + path + "'";
		client.executeQueryAndGetResult(sql1);

		// Assert that new table exists
		assertTrue(client.existTable(tableName));

		// Drop created table
		String sql2 = "drop table " + tableName;
		client.updateQuery(sql2);

		// Assert that the table doesn't exists
		assertFalse(client.existTable(tableName));
	}

	@Test
	public void createAndPurgeExternalTableTest() throws UndefinedTableException, InsufficientPrivilegeException, DuplicateTableException, UnavailableTableLocationException {

		// Create new external table
		client.createExternalTable(tableName, BackendTestingUtil.mockupSchema, path.toUri(), BackendTestingUtil.mockupMeta);

		// Assert that new table exists
		assertTrue(client.existTable(tableName));

		// Drop created table with purge
		client.dropTable(tableName, true);

		// Assert that the table doesn't exists
		assertFalse(client.existTable(tableName));
	}

	private Path createTempTable(String tableName) throws IOException {
		Path tablePath = StorageUtil.concatPath(testDir, tableName);
		BackendTestingUtil.writeTmpTable(conf, tablePath);
		return tablePath;
	}
}
