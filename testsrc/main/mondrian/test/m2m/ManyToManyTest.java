package mondrian.test.m2m;

import mondrian.test.TestContext;
import mondrian.test.loader.CsvDBTestCase;

public class ManyToManyTest  extends CsvDBTestCase {

  private static final String DIRECTORY = "testsrc/main/mondrian/test/m2m";

  private static final String FILENAME = "many_to_many.csv";

  @Override
  protected String getDirectoryName() {
    return DIRECTORY;
  }

  @Override
  protected String getFileName() {
    return FILENAME;
  }
  
  @Override
  protected TestContext createTestContext() {
    final TestContext testContext = TestContext.instance().withSchema(
        "<?xml version=\"1.0\"?>\n"
        + "<Schema name=\"FoodMart\">\n"
        + "  <Dimension name=\"Account\">\n"
        + "    <Hierarchy name=\"Account\" primaryKey=\"id_account\" hasAll=\"true\">\n"
        + "      <Table name=\"m2m_dim_account\"/>\n"
        + "      <Level name=\"Account\" uniqueMembers=\"true\" column=\"id_account\" nameColumn=\"nm_account\" approxRowCount=\"6\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Dimension name=\"CustomerM2M\">\n"
        + "    <Hierarchy name=\"CustomerM2M\" primaryKey=\"id_account\" hasAll=\"true\">\n"
        + "      <Table name=\"m2m_dim_account\" alias=\"m2mAcct\"/>\n"
        + "      <Level name=\"CustomerM2M\" uniqueMembers=\"true\" column=\"id_account\" nameColumn=\"nm_account\" approxRowCount=\"6\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Dimension name=\"Customer\">\n"
        + "    <Hierarchy name=\"Customer\" primaryKey=\"id_account\" primaryKeyTable=\"cAcct\" hasAll=\"true\">\n"
        + "      <Annotations>\n"
        + "         <Annotation name=\"hierarchyType\">m2m</Annotation>\n"
        + "         <Annotation name=\"m2mParent\">CustomerM2M.CustomerM2M</Annotation>\n"
        + "      </Annotations>\n"
        + "      <Join leftKey=\"id_account\" rightKey=\"id_account\">\n"
        + "        <Table name=\"m2m_dim_account\" alias=\"cAcct\"/>\n"
        + "        <Join leftKey=\"id_customer\" rightKey=\"id_customer\">\n"
        + "          <Table name=\"m2m_bridge_accountcustomer\"/>\n"
        + "          <Table name=\"m2m_dim_customer\"/>\n"
        + "        </Join>\n"
        + "      </Join>\n"
        + "      <Level name=\"Customer Name\" table=\"m2m_dim_customer\" uniqueMembers=\"true\" column=\"id_customer\" nameColumn=\"nm_customer\" approxRowCount=\"4\"/>\n"
        + "  </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Dimension name=\"Date\">\n"
        + "    <Hierarchy name=\"Date\" primaryKey=\"ID_Date\" hasAll=\"true\">\n"
        + "      <Table name=\"m2m_dim_date\"/>\n"
        + "      <Level name=\"Date\" uniqueMembers=\"true\" column=\"id_date\" nameColumn=\"nm_date\" approxRowCount=\"2\"/>\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Cube name=\"M2M\">\n"
        + "    <Table name=\"m2m_fact_balance\"/>\n"
        + "    <DimensionUsage name=\"Account\" source=\"Account\" foreignKey=\"id_account\"/>\n"
        + "    <DimensionUsage name=\"CustomerM2M\" source=\"CustomerM2M\" foreignKey=\"id_account\"/>\n"
        + "    <DimensionUsage name=\"Customer\" source=\"Customer\" foreignKey=\"id_account\"/>\n"
        + "    <DimensionUsage name=\"Date\" source=\"Date\" foreignKey=\"id_date\"/>\n"
        + "    <Measure name=\"Amount\" aggregator=\"sum\" column=\"Amount\"/>\n"
        + "    <Measure name=\"Count\" aggregator=\"count\" column=\"Amount\"/>\n"
        + "  </Cube>\n"
        + "</Schema>\n");
    return testContext;
  }

  public void testBasicQuery() {
      final String mdx =
          "Select\n"
          + "{[Measures].[Amount]} on columns,\n"
          + "{[Account].[Account].Members} on rows\n"
          + "From [M2M] WHERE {[Date].[All Dates].[Day 1]}\n";
      getTestContext().assertQueryReturns(
          mdx,
          "Axis #0:\n"
              + "{[Date].[Day 1]}\n"
              + "Axis #1:\n"
              + "{[Measures].[Amount]}\n"
              + "Axis #2:\n"
              + "{[Account].[Luke]}\n"
              + "{[Account].[Mark]}\n"
              + "{[Account].[Mark-Paul]}\n"
              + "{[Account].[Mark-Robert]}\n"
              + "{[Account].[Paul]}\n"
              + "{[Account].[Robert]}\n"
              + "Row #0: 100\n"
              + "Row #1: 100\n"
              + "Row #2: 100\n"
              + "Row #3: 100\n"
              + "Row #4: 100\n"
              + "Row #5: 100\n");
  }
  
  public void testBasicQuery2() {
    final String mdx =
        "Select\n"
        + "{[Account].[Account].Members, [Account].[All Accounts]} on columns,\n"
        + "{[Customer].[Customer Name].Members, [Customer].[All Customers]} on rows\n"
        + "From [M2M] WHERE {[Date].[All Dates].[Day 1]}\n";
    getTestContext().assertQueryReturns(
        mdx,
        "Axis #0:\n"
            + "{[Date].[Day 1]}\n"
            + "Axis #1:\n"
            + "{[Account].[Luke]}\n"
            + "{[Account].[Mark]}\n"
            + "{[Account].[Mark-Paul]}\n"
            + "{[Account].[Mark-Robert]}\n"
            + "{[Account].[Paul]}\n"
            + "{[Account].[Robert]}\n"
            + "{[Account].[All Accounts]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Luke]}\n"
            + "{[Customer].[Mark]}\n"
            + "{[Customer].[Paul]}\n"
            + "{[Customer].[Robert]}\n"
            + "{[Customer].[All Customers]}\n"
            + "Row #0: 100\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 100\n"
            + "Row #1: \n"
            + "Row #1: 100\n"
            + "Row #1: 100\n"
            + "Row #1: 100\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 300\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 100\n"
            + "Row #2: \n"
            + "Row #2: 100\n"
            + "Row #2: \n"
            + "Row #2: 200\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 100\n"
            + "Row #3: \n"
            + "Row #3: 100\n"
            + "Row #3: 200\n"
            + "Row #4: 100\n"
            + "Row #4: 100\n"
            + "Row #4: 100\n"
            + "Row #4: 100\n"
            + "Row #4: 100\n"
            + "Row #4: 100\n"
            + "Row #4: 600\n");
}
  
  public void testBasicQuery3() {
    final String mdx =
        "Select\n"
        + "NON EMPTY {[Account].[Account].Members, [Account].[All Accounts]} on columns,\n"
        + "NON EMPTY {[Customer].[Customer Name].Members, [Customer].[All Customers]} on rows\n"
        + "From [M2M] WHERE {[Date].[All Dates].[Day 2]}\n";
    getTestContext().assertQueryReturns(
        mdx,
        "Axis #0:\n"
            + "{[Date].[Day 2]}\n"
            + "Axis #1:\n"
            + "{[Account].[Luke]}\n"
            + "{[Account].[Mark]}\n"
            + "{[Account].[Mark-Robert]}\n"
            + "{[Account].[Paul]}\n"
            + "{[Account].[Robert]}\n"
            + "{[Account].[All Accounts]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Luke]}\n"
            + "{[Customer].[Mark]}\n"
            + "{[Customer].[Paul]}\n"
            + "{[Customer].[Robert]}\n"
            + "{[Customer].[All Customers]}\n"
            + "Row #0: 105\n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: \n"
            + "Row #0: 105\n"
            + "Row #1: \n"
            + "Row #1: 105\n"
            + "Row #1: 105\n"
            + "Row #1: \n"
            + "Row #1: \n"
            + "Row #1: 210\n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: \n"
            + "Row #2: 105\n"
            + "Row #2: \n"
            + "Row #2: 105\n"
            + "Row #3: \n"
            + "Row #3: \n"
            + "Row #3: 105\n"
            + "Row #3: \n"
            + "Row #3: 105\n"
            + "Row #3: 210\n"
            + "Row #4: 105\n"
            + "Row #4: 105\n"
            + "Row #4: 105\n"
            + "Row #4: 105\n"
            + "Row #4: 105\n"
            + "Row #4: 525\n");
  }
  
  public void testBasicQuery4() {
    final String mdx =
        "Select\n"
        + "NON EMPTY {[Measures].[Amount], [Measures].[Count]} on columns,\n"
        + "NON EMPTY {[Customer].[Customer Name].Members, [Customer].[All Customers]} on rows\n"
        + "From [M2M] WHERE {[Date].[All Dates].[Day 1]}\n";
    getTestContext().assertQueryReturns(
        mdx,
        "Axis #0:\n"
            + "{[Date].[Day 1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Amount]}\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Luke]}\n"
            + "{[Customer].[Mark]}\n"
            + "{[Customer].[Paul]}\n"
            + "{[Customer].[Robert]}\n"
            + "{[Customer].[All Customers]}\n"
            + "Row #0: 100\n"
            + "Row #0: 1\n"
            + "Row #1: 300\n"
            + "Row #1: 3\n"
            + "Row #2: 200\n"
            + "Row #2: 2\n"
            + "Row #3: 200\n"
            + "Row #3: 2\n"
            + "Row #4: 600\n"
            + "Row #4: 6\n");
  }
  
  public void testBasicQuery5() {
      final String mdx =
          "Select\n"
          + "[Measures].[Count] on columns,\n"
          + "[Account].[All Accounts] on rows\n"
          + "From [M2M] WHERE {[Customer].[All Customers].[Mark],[Customer].[All Customers].[Paul]}\n";
      getTestContext().assertQueryReturns(
          mdx,
          "Axis #0:\n"
              + "{[Customer].[Mark]}\n"
              + "{[Customer].[Paul]}\n"
              + "Axis #1:\n"
              + "{[Measures].[Count]}\n"
              + "Axis #2:\n"
              + "{[Account].[All Accounts]}\n"
              + "Row #0: 7\n");
  }

    public void testBasicQuery6() {
      final String mdx =
          "Select\n"
          + "[Measures].[Count] on columns,\n"
          + "[Account].[All Accounts] on rows\n"
          + "From [M2M] WHERE {([Date].[All Dates].[Day 1], [Customer].[All Customers].[Mark]),([Date].[All Dates].[Day 1], [Customer].[All Customers].[Paul])}\n";
      getTestContext().assertQueryReturns(
          mdx,
          "Axis #0:\n"
              + "{[Date].[Day 1], [Customer].[Mark]}\n"
              + "{[Date].[Day 1], [Customer].[Paul]}\n"
              + "Axis #1:\n"
              + "{[Measures].[Count]}\n"
              + "Axis #2:\n"
              + "{[Account].[All Accounts]}\n"
              + "Row #0: 4\n");
  }

    
    public void testBasicQuery7() {
      final String mdx =
          "Select\n"
          + "NON EMPTY {[Measures].[Amount], [Measures].[Count]} on columns,\n"
          + "NON EMPTY {[Account].[All Accounts].Children} on rows\n"
          + "From [M2M] WHERE {([Date].[All Dates].[Day 1], [Customer].[All Customers].[Mark]),([Date].[All Dates].[Day 1], [Customer].[All Customers].[Paul])}\n";
      getTestContext().assertQueryReturns(
          mdx,
          "Axis #0:\n"
              + "{[Date].[Day 1], [Customer].[Mark]}\n"
              + "{[Date].[Day 1], [Customer].[Paul]}\n"
              + "Axis #1:\n"
              + "{[Measures].[Amount]}\n"
              + "{[Measures].[Count]}\n"
              + "Axis #2:\n"
              + "{[Account].[Mark]}\n"
              + "{[Account].[Mark-Paul]}\n"
              + "{[Account].[Mark-Robert]}\n"
              + "{[Account].[Paul]}\n"
              + "Row #0: 100\n"
              + "Row #0: 1\n"
              + "Row #1: 100\n"
              + "Row #1: 1\n"
              + "Row #2: 100\n"
              + "Row #2: 1\n"
              + "Row #3: 100\n"
              + "Row #3: 1\n");
  }

  public void testBasicQuery8() {
    getTestContext().assertQueryReturns(
        "WITH MEMBER [Customer].[Mark and Paul] AS 'AGGREGATE({[Customer].[All Customers].[Mark], [Customer].[All Customers].[Paul]})'\n"
        + "SELECT {[Measures].[Amount], [Measures].[Count]} ON COLUMNS,\n"
        + "      {[Customer].[All Customers].[Mark], [Customer].[All Customers].[Paul], [Customer].[Mark and Paul]} ON ROWS\n"
        + "FROM [M2M]\n"
        + "WHERE ([Date].[All Dates].[Day 1])",
        "Axis #0:\n"
            + "{[Date].[Day 1]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Amount]}\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Mark]}\n"
            + "{[Customer].[Paul]}\n"
            + "{[Customer].[Mark and Paul]}\n"
            + "Row #0: 300\n"
            + "Row #0: 3\n"
            + "Row #1: 200\n"
            + "Row #1: 2\n"
            + "Row #2: 400\n"
            + "Row #2: 4\n");
  }
  
  public void testBasicQuery9() {
    getTestContext().assertQueryReturns(
        "WITH MEMBER [Customer].[Mark and Paul] AS 'AGGREGATE({[Customer].[All Customers].[Mark], [Customer].[All Customers].[Paul]})'\n"
        + "SELECT {[Measures].[Amount], [Measures].[Count]} ON COLUMNS,\n"
        + "      {[Customer].[All Customers].[Mark], [Customer].[All Customers].[Paul], [Customer].[Mark and Paul]} ON ROWS\n"
        + "FROM [M2M]\n"
        + "WHERE {([Date].[All Dates].[Day 1],[Account].[Mark-Paul]),([Date].[All Dates].[Day 1],[Account].[Mark-Robert])}",
        "Axis #0:\n"
            + "{[Date].[Day 1], [Account].[Mark-Paul]}\n"
            + "{[Date].[Day 1], [Account].[Mark-Robert]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Amount]}\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Customer].[Mark]}\n"
            + "{[Customer].[Paul]}\n"
            + "{[Customer].[Mark and Paul]}\n"
            + "Row #0: 200\n"
            + "Row #0: 2\n"
            + "Row #1: 100\n"
            + "Row #1: 1\n"
            + "Row #2: 200\n"
            + "Row #2: 2\n");
  }
  

}
