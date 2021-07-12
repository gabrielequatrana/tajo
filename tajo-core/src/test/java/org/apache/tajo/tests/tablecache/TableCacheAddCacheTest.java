package org.apache.tajo.tests.tablecache;

import static org.junit.Assert.assertTrue;

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
public class TableCacheAddCacheTest {

	// TableCache instance
	private TableCache tableCache;

	// Test parameters
	private TableCacheKey cacheKey;
	private CacheHolder<?> cacheData;
	private Class<? extends Exception> expectedException;
	
	// Testing environment
	private static ExecutionBlockId ebId;
	
	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public TableCacheAddCacheTest(TableCacheTestParameters parameters) {
		this.cacheKey = parameters.getCacheKey();
		this.cacheData = parameters.getCacheData();
		this.expectedException = parameters.getExpectedException();
	}

	@Parameters
	public static Collection<TableCacheTestParameters> getParameters() throws Exception {
		TableCacheKey key;
		ExecutionBlockSharedResource resource = new ExecutionBlockSharedResource();
		List<TableCacheTestParameters> parameters = new ArrayList<>();
		ebId = QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(System.currentTimeMillis(), 0));
		
		// Minimal test suite
		key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, resource).call(), null));
		
		key = null;
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, resource).call(), MultipleFailureException.class));
		
		key = new TableCacheKey("", "", "");
		parameters.add(new TableCacheTestParameters(key, null, null));
		
		// Added after mutation testing
		key = new TableCacheKey("", "testTableCache", "");
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, resource).call(), null));
		
		key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, null).call(), NullPointerException.class));

		return parameters;
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
	
	// Cleanup the test environment
	@After
	public void cleanUp() {
		tableCache.releaseCache(ebId);
	}

	@Test
	public void addCacheTest() {

		// Add data to cache
		tableCache.addCache(cacheKey, cacheData);

		// Assert that the added data exists
		assertTrue(tableCache.hasCache(cacheKey));
	}
}
