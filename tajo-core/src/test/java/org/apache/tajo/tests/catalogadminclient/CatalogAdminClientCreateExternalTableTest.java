package org.apache.tajo.tests.catalogadminclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.DuplicateDatabaseException;
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
public class CatalogAdminClientCreateExternalTableTest {

	// TajoClient instance
	private static TajoClient client;

	// Test parameters
	private String tableName;
	private Schema schema;
	private URI path;
	private TableMeta meta;
	private PartitionMethodDesc partitionMethodDesc;
	private boolean duplicate;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private static TajoTestingCluster cluster;
	private static TajoConf conf;
	private static Path testDir;
	private static Schema validSchema = BackendTestingUtil.mockupSchema;
	private static TableMeta validMeta = BackendTestingUtil.mockupMeta;
	private static PartitionMethodDesc validPartitionMethodDesc = BackendTestingUtil.mockupPartitionMethodDesc;
	private static boolean validUri = true;
	private static boolean invalidUri = false;
	private static URI validPath;
	private static URI invalidPath;

	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public CatalogAdminClientCreateExternalTableTest(String tableName, Schema schema, boolean uri, TableMeta meta, PartitionMethodDesc partitionMethodDesc, boolean duplicate, Class<? extends Exception> expectedException) throws IOException, URISyntaxException {
		this.tableName = tableName;
		this.schema = schema;
		if (uri) {
			this.path = validPath;
		} else {
			this.path = invalidPath;
		}
		this.meta = meta;
		this.partitionMethodDesc = partitionMethodDesc;
		this.duplicate = duplicate;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() throws IOException, URISyntaxException {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ "test_table", validSchema, validUri, validMeta, validPartitionMethodDesc, false, null }, 
			{ "test_table", null, validUri, validMeta, null, false, null },
			{ "", validSchema, validUri, validMeta, null, false, null },
			{ "test_table", validSchema, invalidUri, validMeta, validPartitionMethodDesc, false, UnavailableTableLocationException.class },
			
			// Added after the improvement of the test suite
			{ "test_table", validSchema, validUri, validMeta, validPartitionMethodDesc, true, DuplicateTableException.class }
		});
	}

	// Setup the test environment
	@BeforeClass
	public static void setUp() throws Exception {
		cluster = TpchTestBase.getInstance().getTestingCluster();
		conf = cluster.getConfiguration();
		testDir = CommonTestingUtil.getTestDir();
		client = cluster.newTajoClient();
		validPath = CatalogAdminClientTestUtil.validUri(testDir, conf);
		invalidPath = CatalogAdminClientTestUtil.invalidUri();
	}
	
	@Before
	public void configuration() throws DuplicateDatabaseException {
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
	public void createExternalTableTest() throws DuplicateTableException, UnavailableTableLocationException, InsufficientPrivilegeException {
		
		// Create new external table
		TableDesc table = client.createExternalTable(tableName, schema, path, meta, partitionMethodDesc);
		
		if (duplicate) {
			
			// Try to create a table with the same name
			client.createExternalTable(tableName, schema, path, meta, partitionMethodDesc);
		}

		// Assert that new table exists
		assertTrue(client.existTable(tableName));
	}
}
