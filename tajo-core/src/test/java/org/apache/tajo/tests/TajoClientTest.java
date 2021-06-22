package org.apache.tajo.tests;

import org.apache.tajo.tests.tajo.client.tests.TajoClientDatabaseTest;
import org.apache.tajo.tests.tajo.client.tests.TajoClientSessionVariableTest;
import org.apache.tajo.tests.tajo.client.tests.TajoClientTableTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TajoClientDatabaseTest.class, TajoClientSessionVariableTest.class, TajoClientTableTest.class })
public class TajoClientTest {

}
