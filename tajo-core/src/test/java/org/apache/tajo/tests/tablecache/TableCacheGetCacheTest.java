package org.apache.tajo.tests.tablecache;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.tests.util.TableCacheTestParameters;
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
import org.junit.runners.model.MultipleFailureException;

@RunWith(Parameterized.class)
public class TableCacheGetCacheTest {

	// TableCache instance
	private TableCache tableCache;

	// Test parameters
	private TableCacheKey cacheKey;
	private Class<? extends Exception> expectedException;

	// Testing environment
	private static ExecutionBlockId ebId;
	private ExecutionBlockSharedResource resource;
	private CacheHolder<?> cacheData;
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public TableCacheGetCacheTest(TableCacheTestParameters parameters) {
		this.cacheKey = parameters.getCacheKey();
		this.expectedException = parameters.getExpectedException();
	}

	@Parameters
	public static Collection<TableCacheTestParameters> getParameters() {
		TableCacheKey key;
		List<TableCacheTestParameters> parameters = new ArrayList<>();
		ebId = QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(System.currentTimeMillis(), 0));

		// Minimal test suite
		key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
		parameters.add(new TableCacheTestParameters(key, null));
		
		key = new TableCacheKey("", "", "");
		parameters.add(new TableCacheTestParameters(key, null));
		
		key = null;
		parameters.add(new TableCacheTestParameters(key, MultipleFailureException.class));

		// Added after mutation testing
		key = new TableCacheKey(ebId.toString(), "", "path");
		parameters.add(new TableCacheTestParameters(key, null));

		return parameters;
	}

	// Setup the test environment
	@Before
	public void setUp() throws Exception {
		TableCacheTestUtil.reset();
		tableCache = TableCache.getInstance();
		resource = new ExecutionBlockSharedResource();
		cacheData = TableCacheTestUtil.createCacheData(cacheKey, resource).call();
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
	}

	// Cleanup the test environment
	@After
	public void cleanUp() {
		tableCache.releaseCache(ebId);
	}

	@Test
	public void getCacheTest() {
		
		// Add data to cache
		tableCache.addCache(cacheKey, cacheData);

		// Get data from the cache
		CacheHolder<?> actualData = tableCache.getCache(cacheKey);

		// Assert that retrieved data is the same as the added data
		assertEquals(cacheData, actualData);
	}
}
