package org.apache.tajo.tests;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.tests.util.TajoTestingCluster;
import org.apache.tajo.tests.util.TpchTestBase;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TryTest {
	
	private static TajoTestingCluster cluster;
	private static TajoConf conf;
	private static TajoClient client;
	private static Path testDir;
	
	@BeforeClass
	public static void setUp() throws Exception {
		System.setProperty("hadoop.home.dir", "D:/Software/Eclipse/winutils");
		cluster = TpchTestBase.getInstance().getTestingCluster();
		conf = cluster.getConfiguration();
		client = cluster.newTajoClient();
		testDir = CommonTestingUtil.getTestDir();
	}
	
	@AfterClass
	public static void cleanUp() {
		client.close();
	}
	
	@Test
	public void createDatabaseTest() throws DuplicateDatabaseException {
		System.out.println("TESTTESTETESTESTESTESTESTESTESTESTESTESTESTEST");
		int current = client.getAllDatabaseNames().size();
		
		String p = IdentifierUtil.normalizeIdentifier("testCreateDatabase_");
		for (int i = 0; i < 10; i++) {
			assertEquals(current+i, client.getAllDatabaseNames().size());
			
			client.createDatabase(p+i);;
			
			assertEquals(current+i+1, client.getAllDatabaseNames().size());
		}
	}

}
