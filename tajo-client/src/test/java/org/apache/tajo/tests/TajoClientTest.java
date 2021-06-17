package org.apache.tajo.tests;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

//@RunWith(Parameterized.class)
public class TajoClientTest {
	
	private static TajoConf conf;
	private static TajoClient client;
	private static Path testDir;
	
	@BeforeClass
	public static void setUp() throws SQLException, IOException {
		conf = getConfiguration();
		client = newTajoClient(conf);
		testDir = getTestDir();
	}
	
	@AfterClass
	public static void tearDown() {
		client.close();
	}

	@Test
	public void testCreateDatabase() throws TajoException {
		int current = client.getAllDatabaseNames().size();
		String prefix = IdentifierUtil.normalizeIdentifier("testCreateDatabase_");
		
		for (int i = 0; i < 10; i++) {
			assertFalse(client.existDatabase(prefix + i));
			client.createDatabase(prefix + i);
			assertTrue(client.existDatabase(prefix + i));
			
			assertEquals(current + i + 1, client.getAllDatabaseNames().size());
			assertTrue(client.getAllDatabaseNames().contains(prefix + i));
		}
		
		for (int i = 0; i < 10; i++) {
			client.dropDatabase(prefix + i);
			assertFalse(client.existDatabase(prefix + i));
			assertFalse(client.getAllDatabaseNames().contains(prefix + i));
		}
		
		assertEquals(current, client.getAllDatabaseNames().size());
	}
	
	private static TajoClient newTajoClient(TajoConf conf) throws SQLException {
		return new TajoClientImpl(ServiceTrackerFactory.get(conf));
	}
	
	private static TajoConf getConfiguration() {
		TajoConf conf = new TajoConf();
		conf.setBoolVar(ConfVars.TAJO_MASTER_HA_ENABLE, false);
		
		return conf;
	}

	private static Path getTestDir() throws IOException {
		String rand = UUID.randomUUID().toString();
		Path path = new Path("target/test-data", rand);
		FileSystem fs = FileSystem.getLocal(new Configuration());
		
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		
		fs.mkdirs(path);
		
		return fs.makeQualified(path);
	}
}
