package org.apache.tajo.tests;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
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
	private static int param = 0;
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public TableCacheAddCacheTest(TableCacheKey cacheKey, CacheHolder<?> cacheData, Class<? extends Exception> expectedException) {
		this.cacheKey = cacheKey;
		this.cacheData = cacheData;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() throws Exception {
		TableCacheKey key;
		ExecutionBlockSharedResource resource;
		
		switch(param) {
		case 0:
			key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
			resource = new ExecutionBlockSharedResource();
			break;
			
		case 1:
			key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
			resource = null;
			break;
			
		case 2:
			key = new TableCacheKey(ebId.toString(), "", "path");
			resource = new ExecutionBlockSharedResource();
			break;
			
		case 3:
			key = new TableCacheKey(null, "testTableCache", "");
			resource = new ExecutionBlockSharedResource();
			break;
			
		case 4:
			key = null;
			resource = new ExecutionBlockSharedResource();
			break;
	
		default:
			key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
			resource = new ExecutionBlockSharedResource();
			break;
		}
		param++;
		
		return Arrays.asList(new Object[][] { 
			{ key, TableCacheTestUtil.createCacheData(key, resource).call(), null },
			{ key, TableCacheTestUtil.createCacheData(key, resource).call(), null },
			{ key, TableCacheTestUtil.createCacheData(key, resource).call(), null },
			{ key, TableCacheTestUtil.createCacheData(key, resource).call(), null },
			{ key, TableCacheTestUtil.createCacheData(key, resource).call(), null }
		});
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
