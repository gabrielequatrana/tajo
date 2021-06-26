package org.apache.tajo.tests.util;

import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCacheKey;

public class TableCacheTestParameters {
	private TableCacheKey cacheKey;
	private CacheHolder<?> cacheData;
	private Class<? extends Exception> expectedException;

	public TableCacheTestParameters(TableCacheKey cacheKey, CacheHolder<?> cacheData, Class<? extends Exception> expectedException) {
		this.setCacheKey(cacheKey);
		this.setCacheData(cacheData);
		this.setExpectedException(expectedException);
	}
	
	public TableCacheTestParameters(TableCacheKey cacheKey, Class<? extends Exception> expectedException) {
		this.setCacheKey(cacheKey);
		this.setExpectedException(expectedException);
	}

	public TableCacheKey getCacheKey() {
		return cacheKey;
	}

	public void setCacheKey(TableCacheKey cacheKey) {
		this.cacheKey = cacheKey;
	}

	public CacheHolder<?> getCacheData() {
		return cacheData;
	}

	public void setCacheData(CacheHolder<?> cacheData) {
		this.cacheData = cacheData;
	}

	public Class<? extends Exception> getExpectedException() {
		return expectedException;
	}

	public void setExpectedException(Class<? extends Exception> expectedException) {
		this.expectedException = expectedException;
	}
}
