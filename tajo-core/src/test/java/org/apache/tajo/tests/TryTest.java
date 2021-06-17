package org.apache.tajo.tests;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;

import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.function.builtin.Date;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

public class TryTest {
	
	@Test
	public void test() {
		Date date = new Date();
		Tuple tuple = new VTuple(new Datum[] {new TextDatum("25/12/2012 00:00:00")});
		Int8Datum unixtime = (Int8Datum) date.eval(tuple);
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(unixtime.asInt8());
	    assertEquals(2012, c.get(Calendar.YEAR));
	    assertEquals(11, c.get(Calendar.MONTH));
	    assertEquals(25, c.get(Calendar.DAY_OF_MONTH));
	    assertEquals(0, c.get(Calendar.HOUR_OF_DAY));
	    assertEquals(0, c.get(Calendar.MINUTE));
	    assertEquals(0, c.get(Calendar.SECOND));
	}

}
