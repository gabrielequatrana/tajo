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
import org.apache.tajo.worker.ExecutionBlockSharedResource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized.Parameters;

public class TableCacheTest {

	// TableCache instance
	private TableCache tableCache;

	// Test parameters
	private TableCacheKey cacheKey;
	private CacheHolder<?> cacheData;
	private Class<? extends Exception> expectedException;

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public TableCacheTest(TableCacheKey cacheKey, CacheHolder<?> cacheData, Class<? extends Exception> expectedException) {
		this.cacheKey = cacheKey;
		this.cacheData = cacheData;
		this.expectedException = expectedException;
	}

	@Parameters
	public Collection<Object[]> getParameters() {
		ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(QueryIdFactory.newQueryId(System.currentTimeMillis(), 0));
		TableCacheKey key = new TableCacheKey(ebId.toString(), "testTableCache", "path");
		ExecutionBlockSharedResource resource = new ExecutionBlockSharedResource();
		
		return Arrays.asList(new Object[][] { 
			{ key, createTask(key, resource) } 
		});
	}

	@Test
	public void addCacheTest() {
		System.out.println("ADD");
		tableCache.addCache(cacheKey, cacheData);
		
		System.out.println("RESULT");
		System.out.println("A: " + tableCache.hasCache(cacheKey));
		
		assertTrue(tableCache.hasCache(cacheKey));
	}

	private Callable<CacheHolder<Long>> createTask(TableCacheKey key, ExecutionBlockSharedResource resource) {
		return new Callable<CacheHolder<Long>>() {
			@Override
			public CacheHolder<Long> call() throws Exception {
				CacheHolder<Long> result;
				synchronized (resource.getLock()) {
					if (!TableCache.getInstance().hasCache(key)) {
						final long nanoTime = System.nanoTime();
						final TableStats tableStats = new TableStats();
						tableStats.setNumRows(100);
						tableStats.setNumBytes(1000);

						final CacheHolder<Long> cacheHolder = new CacheHolder<Long>() {

							@Override
							public Long getData() {
								return nanoTime;
							}

							@Override
							public TableStats getTableStats() {
								return tableStats;
							}

							@Override
							public void release() {

							}
						};

						resource.addBroadcastCache(key, cacheHolder);
					}
				}

				CacheHolder<?> holder = resource.getBroadcastCache(key);
				result = (CacheHolder<Long>) holder;
				return result;
			}
		};
	}
}
