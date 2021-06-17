import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.schema.Field;
import org.apache.tajo.schema.Identifier;
import org.apache.tajo.schema.QualifiedIdentifier;
import org.apache.tajo.schema.Schema;
import org.apache.tajo.tuple.memory.UnSafeTuple;
import org.apache.tajo.tuple.memory.UnSafeTupleList;
import org.apache.tajo.type.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
class RadixSortTest {
	
	private QueryContext queryContext;
	private UnSafeTupleList tupleList;
	private Schema schema;
	private SortSpec[] sortSpecs;
	private Comparator<UnSafeTuple> comp;
	
	public RadixSortTest(QueryContext queryContext, UnSafeTupleList tupleList, Schema schema, SortSpec[] sortSpecs, Comparator<UnSafeTuple> comp) {
		this.queryContext = queryContext;
		this.tupleList = tupleList;
		this.schema = schema;
		this.sortSpecs = sortSpecs;
		this.comp = comp;
	}
	
	@Parameters
	public Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ getQueryContext() }
		});
	}
	
	@Before
	public void setUp() {
		
	}

	@Test
	public void testSort() {
		
	}
	
	@After
	public void cleanUp() {
		
	}
	
	private QueryContext getQueryContext() {
		QueryContext queryContext = new QueryContext(new TajoConf());
		queryContext.setInt(SessionVars.TEST_TIM_SORT_THRESHOLD_FOR_RADIX_SORT, 0);
		
		return queryContext;
	}
	
	private Schema getSchema() {

		Schema schema = SchemaBuilder.builder().add(new Field(QualifiedIdentifier.$("col0"), Type.Int8)).buildV2();

		/*
		Schema schema = SchemaBuilder.builder().add.addAll(new Column[] {
				new Column("col0", Type.INT8),
				new Column("col1", Type.INT4)
		}).build();*/
		
		return schema;
	}
}
