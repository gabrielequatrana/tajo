package org.apache.tajo.tests;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.tests.util.TableCacheTestParameters;
import org.apache.tajo.tests.util.TableCacheTestUtil;
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TableCacheAddCacheTest {

	// TableCache instance
	private static TableCache tableCache;

	// Test parameters
	private TableCacheKey cacheKey;
	private CacheHolder<?> cacheData;
	private Class<? extends Exception> expectedException;
	
	// Testing environment
	private static ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(System.currentTimeMillis(), 0));
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

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
		
		key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, resource).call(), null));
		key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, null).call(), NullPointerException.class));
		key = new TableCacheKey(ebId.toString(), "", "path");
		parameters.add(new TableCacheTestParameters(key, null, NullPointerException.class));
		key = new TableCacheKey(null, "testTableCache", "");
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, resource).call(), NullPointerException.class));
		key = null;
		parameters.add(new TableCacheTestParameters(key, TableCacheTestUtil.createCacheData(key, resource).call(), NullPointerException.class));
		
		return parameters;
	}
	
	@BeforeClass
	public static void setUp() {
		tableCache = TableCache.getInstance();
	}
	
	@After
	public void cleanUp() {
		tableCache.releaseCache(ebId);
	}

	@Test
	public void addCacheTest() {
		System.out.println("\n*************** TEST ***************");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Raised exception: " + expectedException.getName());
		}

		tableCache.addCache(cacheKey, cacheData);
		
		System.out.println("\n-------------- ADD --------------");
		System.out.println("Cache key: " + cacheKey.toString());
		System.out.println("Cache data size: " + cacheData.toString());
		System.out.println("Has cache: " + tableCache.hasCache(cacheKey));
		
		assertTrue(tableCache.hasCache(cacheKey));
		
		System.out.println("\n************************************\n");
	}
}
