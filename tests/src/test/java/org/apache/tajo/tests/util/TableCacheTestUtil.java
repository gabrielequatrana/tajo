package org.apache.tajo.tests.util;

import java.util.concurrent.Callable;

import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.worker.ExecutionBlockSharedResource;

public class TableCacheTestUtil {
	
	private TableCacheTestUtil() {
		
	}

	public static Callable<CacheHolder<Long>> createCacheData(TableCacheKey key, ExecutionBlockSharedResource resource) {
		return new Callable<CacheHolder<Long>>() {
			@Override
			public CacheHolder<Long> call() throws Exception {
				if (key == null || resource == null) {
					return null;
				}
				
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

				return resource.getBroadcastCache(key);
			}
		};
	}
	
}
