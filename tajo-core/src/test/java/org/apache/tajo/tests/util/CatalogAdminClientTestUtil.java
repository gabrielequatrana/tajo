package org.apache.tajo.tests.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;

public class CatalogAdminClientTestUtil {
	
	private CatalogAdminClientTestUtil() {
		
	}

	public static URI validUri(Path testDir, TajoConf conf) throws IOException {
		Path tablePath = StorageUtil.concatPath(testDir, "test_table");
		BackendTestingUtil.writeTmpTable(conf, tablePath);
		return tablePath.toUri();
	}
	
	public static URI invalidUri() throws URISyntaxException {
		return new URI("test_table");
	}

	public static Path createTempTable(String tableName, Path testDir, TajoConf conf) throws IOException {
		Path tablePath = StorageUtil.concatPath(testDir, tableName);
		BackendTestingUtil.writeTmpTable(conf, tablePath);
		return tablePath;
	}
}
