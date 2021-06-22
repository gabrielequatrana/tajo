package org.apache.tajo.tests;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.Test;

public class TryTest {

	@Test
	public void test() throws SQLException {
		TajoClient t = new TajoClientImpl(ServiceTrackerFactory.get(new TajoConf()));
		System.out.println(t);
		
		assertEquals(2,2);
	}
}
