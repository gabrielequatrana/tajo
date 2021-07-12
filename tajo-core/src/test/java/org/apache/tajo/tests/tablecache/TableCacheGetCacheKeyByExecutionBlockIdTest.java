package org.apache.tajo.tests.tablecache;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.tests.util.TableCacheTestUtil;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TableCacheGetCacheKeyByExecutionBlockIdTest {

	// TableCache instance
	private TableCache tableCache;

	// Test parameters
	private ExecutionBlockId ebId;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	ExecutionBlockSharedResource resource;
	TableCacheKey cacheKey1;
	TableCacheKey cacheKey2;
	TableCacheKey cacheKey3;
	CacheHolder<?> cacheData1;
	CacheHolder<?> cacheData2;
	CacheHolder<?> cacheData3;
	List<TableCacheKey> cacheKeys;
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public TableCacheGetCacheKeyByExecutionBlockIdTest(ExecutionBlockId ebId, Class<? extends Exception> expectedException) {
		this.ebId = ebId;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(System.currentTimeMillis(), 0)), null },
			{ QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(-1L, 0)), null },
			{ null, NullPointerException.class },
			
			// Added after mutation testing
			//{ QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(System.currentTimeMillis(), -1)), null },
		});
	}

	// Setup the test environment
	@Before
	public void setUp() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		TableCacheTestUtil.reset();
		tableCache = TableCache.getInstance();
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
	}
	
	@Before
	public void configuration() throws Exception {
		if (ebId == null) {
			return;
		}
		
		resource = new ExecutionBlockSharedResource();
		
		cacheKey1 = new TableCacheKey(ebId.toString(), "test1", "path1");
		cacheKey2 = new TableCacheKey(ebId.toString(), "test2", "path2");
		cacheKey3 = new TableCacheKey(ebId.toString(), "test3", "path3");
		
		cacheKeys = new ArrayList<>();
		cacheKeys.add(cacheKey1);
		cacheKeys.add(cacheKey2);
		cacheKeys.add(cacheKey3);
		
		cacheData1 = TableCacheTestUtil.createCacheData(cacheKey1, resource).call();
		cacheData2 = TableCacheTestUtil.createCacheData(cacheKey2, resource).call();
		cacheData3 = TableCacheTestUtil.createCacheData(cacheKey3, resource).call();
	}

	// Cleanup the test environment
	@After
	public void cleanUp() {
		tableCache.releaseCache(ebId);
	}

	@Test
	public void getCacheKeyByExecutionBlockIdTest() {

		// Add data to the cache
		tableCache.addCache(cacheKey1, cacheData1);
		tableCache.addCache(cacheKey2, cacheData2);
		tableCache.addCache(cacheKey3, cacheData3);

		// Retrieve the list of keys from the cache
		List<TableCacheKey> actualKeys = tableCache.getCacheKeyByExecutionBlockId(ebId);
		for (int i = 0; i < 3; i++) {
			int index = actualKeys.indexOf(cacheKeys.get(i));
			
			// Assert that the cache key is equal to the one retrieved from the cache
			assertEquals(cacheKeys.get(i), actualKeys.get(index));
		}
	}
}
