package org.apache.tajo.tests;

import static org.junit.Assert.assertEquals;
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
import org.junit.BeforeClass;
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
	private static TableCache tableCache;

	// Test parameters
	private TableCacheKey cacheKey;
	private Class<? extends Exception> expectedException;

	// Testing environment
	private static ExecutionBlockId ebId;
	private static ExecutionBlockSharedResource resource;
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public TableCacheGetCacheTest(TableCacheTestParameters parameters) {
		this.cacheKey = parameters.getCacheKey();
		this.expectedException = parameters.getExpectedException();
	}

	@Parameters
	public static Collection<TableCacheTestParameters> getParameters() throws Exception {
		TableCacheKey key;
		List<TableCacheTestParameters> parameters = new ArrayList<>();
		ebId = QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(System.currentTimeMillis(), 0));

		key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
		parameters.add(new TableCacheTestParameters(key, null));
		key = new TableCacheKey("", "testTableCache", "");
		parameters.add(new TableCacheTestParameters(key, null));
		key = new TableCacheKey(ebId.toString(), "", "path");
		parameters.add(new TableCacheTestParameters(key, null));
		key = null;
		parameters.add(new TableCacheTestParameters(key, MultipleFailureException.class));

		return parameters;
	}

	@BeforeClass
	public static void setUp() {
		tableCache = TableCache.getInstance();
		resource = new ExecutionBlockSharedResource();
	}

	@After
	public void cleanUp() {
		tableCache.releaseCache(ebId);
	}

	@Test
	public void getCacheTest() throws Exception {
		System.out.println("\n*************** TEST ***************");

		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Raised exception: " + expectedException.getName());
		}
		
		CacheHolder<?> cacheData = TableCacheTestUtil.createCacheData(cacheKey, resource).call();
		tableCache.addCache(cacheKey, cacheData);

		System.out.println("\n-------------- ADD --------------");
		System.out.println("Cache key: " + cacheKey.toString());
		System.out.println("Cache data size: " + cacheData.toString());
		System.out.println("Has cache: " + tableCache.hasCache(cacheKey));

		assertTrue(tableCache.hasCache(cacheKey));
		
		CacheHolder<?> actualData = tableCache.getCache(cacheKey);
		
		System.out.println("\n-------------- GET --------------");
		System.out.println("Cache key: " + cacheKey.toString());
		System.out.println("Cache data size: " + actualData.toString());
		
		System.out.println("\n------------- RESULT -------------");
		System.out.println("Expected data: " + cacheData.toString());
		System.out.println("Actual data: " + actualData.toString());
		
		assertEquals(cacheData, actualData);

		System.out.println("\n************************************\n");
	}
}
