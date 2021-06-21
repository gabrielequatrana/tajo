package org.apache.tajo.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.CannotDropCurrentDatabaseException;
import org.apache.tajo.exception.DuplicateTableException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UnavailableTableLocationException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.tests.util.BackendTestingUtil;
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
public class TajoClientTableTest {
	
	private static TajoTestingCluster cluster;
	private static TajoConf conf;
	private static TajoClient client;
	private static Path testDir;

	private String tableName;

	public TajoClientTableTest(String tableName) {
		this.tableName = tableName;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ "test_table" }, 
			{ "" }
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
	public void cleanUp() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, UndefinedTableException {
		int tables = client.getTableList(null).size();
		if (tables > 0) {
			for (String str : client.getTableList(null)) {
				client.dropTable(str);;
			}
		}
	}
	
	@Test
	public void createAndDropExternalTableTest() throws UndefinedTableException, InsufficientPrivilegeException, DuplicateTableException, UnavailableTableLocationException, IOException {
		System.out.println("\n*************** TEST ***************");	
		int after = client.getTableList(null).size();
		
		Path path = createTempTable(tableName);
		client.createExternalTable(tableName, BackendTestingUtil.mockupSchema, path.toUri(), BackendTestingUtil.mockupMeta);
	
		System.out.println("\n-------------- CREATE --------------");
		System.out.println("Created table: " + tableName);
		System.out.println("N. of tables: " + client.getTableList(null).size());
		
		assertTrue(client.existTable(tableName));

		client.dropTable(tableName);
		
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
	public void createAndDropExternalTableByQueryTest() throws TajoException, IOException {
		System.out.println("\n*************** TEST ***************");	
		int after = client.getTableList(null).size();
		
		Path path = createTempTable(tableName);
		String sql1 = "create external table " + tableName + " (deptname text, score int4) " 
					+ "using csv location '" + path + "'";
		
		client.executeQueryAndGetResult(sql1);
		
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
	
	private Path createTempTable(String tableName) throws IOException {
		Path tablePath = StorageUtil.concatPath(testDir, tableName);
		BackendTestingUtil.writeTmpTable(conf, tablePath);
		return tablePath;
	}
}
