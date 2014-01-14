/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
*/
package mondrian.test.m2m;

import mondrian.test.TestContext;
import mondrian.test.loader.CsvDBLoader.ListRowStream;
import mondrian.test.loader.CsvDBTestCase;
import mondrian.test.loader.DBLoader.Row;
import mondrian.test.loader.DBLoader.RowDefault;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the beginnings of a many to many performance test setup.
 * It takes the initial customer, account scenario and generates data.
 *
 * For now, this should not be part of the core test suite, as it's more for
 * developer testing of performance.
 *
 * @author Will Gorman (wgorman@pentaho.com)
 *
 */
public class ManyToManyPerformanceTest  extends CsvDBTestCase {

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
            + "\n"
            + "  <Dimension name=\"Account\">\n"
            + "    <Hierarchy name=\"Account\" primaryKey=\"id_account\" hasAll=\"true\">\n"
            + "      <Table name=\"m2m_dim_account\"/>\n"
            + "      <Level name=\"Account\" uniqueMembers=\"true\" column=\"id_account\" nameColumn=\"nm_account\" approxRowCount=\"6\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Dimension name=\"Customer\">\n"
            + "    <Hierarchy name=\"Customer\" primaryKey=\"id_customer\" hasAll=\"true\">\n"
            + "      <Table name=\"m2m_dim_customer\"/>\n"
            + "      <Level name=\"Customer Name\" uniqueMembers=\"true\" column=\"id_customer\" nameColumn=\"nm_customer\" approxRowCount=\"4\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Dimension name=\"Date\">\n"
            + "    <Hierarchy name=\"Date\" primaryKey=\"ID_Date\" hasAll=\"true\">\n"
            + "      <Table name=\"m2m_dim_date\"/>\n"
            + "      <Level name=\"Date\" uniqueMembers=\"true\" column=\"id_date\" nameColumn=\"nm_date\" approxRowCount=\"2\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Cube name=\"CustomerAccountBridge\" visible=\"false\">\n"
            + "    <Table name=\"m2m_bridge_accountcustomer\"/>\n"
            + "    <DimensionUsage name=\"Customer\" source=\"Customer\" foreignKey=\"id_customer\"/>\n"
            + "    <DimensionUsage name=\"Account\" source=\"Account\" foreignKey=\"id_account\"/>\n"
            + "    <Measure name=\"Count\" aggregator=\"count\" column=\"id_customer\"/>\n"
            + "  </Cube>"
            + "\n"
            + "  <Cube name=\"M2M\">\n"
            + "    <Table name=\"m2m_fact_balance\">\n"
            + "      <AggName name=\"m2m_fact_balance_date_agg\">\n"
            + "        <AggFactCount column=\"fact_count\"/>\n"
            + "        <AggForeignKey factColumn=\"id_account\" aggColumn=\"id_account\" />\n"
            + "        <AggMeasure name=\"[Measures].[Amount]\" column=\"amount_sum\"/>\n"
            + "        <AggMeasure name=\"[Measures].[Count]\" column=\"amount_count\" />\n"
            + "      </AggName>\n"
            + "    </Table>\n"
            + "    <DimensionUsage name=\"Account\" source=\"Account\" foreignKey=\"id_account\"/>\n"
            + "    <DimensionUsage name=\"Customer\" source=\"Customer\" foreignKey=\"id_account\" bridgeCube=\"CustomerAccountBridge\"/>\n"
            + "    <DimensionUsage name=\"Date\" source=\"Date\" foreignKey=\"id_date\"/>\n"
            + "    <Measure name=\"Amount\" aggregator=\"sum\" column=\"Amount\"/>\n"
            + "    <Measure name=\"Count\" aggregator=\"count\" column=\"Amount\"/>\n"
            + "    <Measure name=\"Distinct Account Count\" aggregator=\"distinct-count\" column=\"id_account\"/>\n"
            + "  </Cube>\n"
            + "\n"
            + "  <Cube name=\"M2MCount\">\n"
            + "    <Table name=\"m2m_fact_count\"/>\n"
            + "    <DimensionUsage name=\"Account\" source=\"Account\" foreignKey=\"id_account_diff\"/>\n"
            + "    <DimensionUsage name=\"Customer\" source=\"Customer\" foreignKey=\"id_account_diff\" bridgeCube=\"CustomerAccountBridge\"/>\n"
            + "    <Measure name=\"Total Count\" aggregator=\"count\" column=\"total\"/>\n"
            + "  </Cube>"
            + "\n"
            + "  <VirtualCube name=\"M2MVirtual\" defaultMeasure=\"Amount\">\n"
            + "    <CubeUsages>\n"
            + "      <CubeUsage cubeName=\"M2M\" ignoreUnrelatedDimensions=\"true\"/>\n"
            + "      <CubeUsage cubeName=\"M2MCount\" ignoreUnrelatedDimensions=\"true\"/>\n"
            + "    </CubeUsages>\n"
            + "    <VirtualCubeDimension name=\"Account\"/>\n"
            + "    <VirtualCubeDimension name=\"Customer\"/>\n"
            + "    <VirtualCubeDimension name=\"Date\"/>\n"
            + "    <VirtualCubeMeasure cubeName=\"M2M\" name=\"[Measures].[Amount]\"/>\n"
            + "    <VirtualCubeMeasure cubeName=\"M2MCount\" name=\"[Measures].[Total Count]\"/>\n"
            + "  </VirtualCube>"
            + "</Schema>\n");
        return testContext;
    }

    // these are nobs that can be adjusted to scale up and down the test
    // scenarios.

    static final int numCustomers = 350;
    static final double acctMultiplier = 2;
    static final int numAccounts = (int)(numCustomers * acctMultiplier);
    static final int numDates = 2;
    static final int customerSetSize = 10;
    String customerSet = "";
    String customerSetResult = "";
    String expectedCustomerSetSize = "40";
    static final int facts = numAccounts * numDates;

    @Override
    protected void initTables() {
        super.initTables();
        Map<String, List<Row>> tableNameToRowsMap =
            new HashMap<String, List<Row>>();
        List<Row> customerRows = new ArrayList<Row>();
        List<Row> accountRows = new ArrayList<Row>();
        List<Row> bridgeRows = new ArrayList<Row>();
        List<Row> dateRows = new ArrayList<Row>();
        List<Row> factRows = new ArrayList<Row>();

        tableNameToRowsMap.put("m2m_dim_customer", customerRows);
        tableNameToRowsMap.put("m2m_dim_account", accountRows);
        tableNameToRowsMap.put("m2m_bridge_accountcustomer", bridgeRows);
        tableNameToRowsMap.put("m2m_dim_date", dateRows);
        tableNameToRowsMap.put("m2m_fact_balance", factRows);

        for (int custNum = 0 ; custNum < numCustomers; custNum++) {
            customerRows.add(new RowDefault(
                new Object[] {
                    custNum, "Location " + custNum % 10, "C " + custNum}));
            accountRows.add(
                new RowDefault(
                    new Object[] {custNum, "One Person", "C " + custNum}));
            if (custNum <= customerSetSize && custNum != 0) {
                if (custNum != 1) {
                  customerSet += ",";
                }
                customerSet += "[Customer].[C " + custNum + "]";
                customerSetResult += "\n{[Customer].[C " + custNum + "]}";
            }
            bridgeRows.add(new RowDefault(new Object[] {custNum, custNum}));
        }
        int remainingAccounts = numAccounts - numCustomers;
        int acctSize = 2;
        while (remainingAccounts > 0) {
            int acctNum = numAccounts - remainingAccounts;
            int custs[] = new int[acctSize];

            // initial value
            for (int j = 0; j < acctSize; j++) {
                custs[j] = j;
            }

            while (remainingAccounts > 0) {
                // populate db
                String acctName = "";
                for (int l = 0; l < acctSize; l++) {
                    if (l != 0) {
                        acctName += ",";
                    }
                    acctName += "C" + custs[l];
                    bridgeRows.add(
                        new RowDefault(new Object[] {acctNum, custs[l]}));
                }
                accountRows.add(
                    new RowDefault(
                        new Object[] {
                            acctNum, "" + acctSize + " People", acctName}));
                acctNum++;
                remainingAccounts--;

                // iterate to the next account
                for (int j = acctSize - 1; j >= 0; j--) {
                    if (custs[j] < numCustomers) {
                        custs[j]++;
                        break;
                    } else {
                        if (j == 0) {
                            // we are done with this acctSize
                            custs = new int[++acctSize];
                            // initial value
                            for (int i = 0; i < acctSize; i++) {
                                custs[i] = i;
                            }
                            break;
                        }
                        custs[j] = custs[j - 1] + 1;
                    }
                }
            }
        }

        for (int i = 0; i < numDates; i++) {
            dateRows.add(new RowDefault(new Object[] {i, "Day " + i}));
        }

        for (int dateNum = 0; dateNum < numDates; dateNum++) {
            for (int accountNum = 0; accountNum < numAccounts; accountNum++) {
                factRows.add(
                    new RowDefault(new Object[] {accountNum, dateNum, 100}));
            }
        }

        System.out.println(
            "Populating database with # accts: "
            + accountRows.size() + ", # customers: "
            + customerRows.size() + ", # in bridge: "
            + bridgeRows.size() + ", # in date: "
            + dateRows.size() + ", # in fact: "
            + factRows.size());

        for (int i = 0; i < this.tables.length; i++) {
            List<Row> rows = tableNameToRowsMap.get(this.tables[i].getName());
            if (rows != null) {
                if (this.tables[i].getController().getRowStream()
                    instanceof ListRowStream)
                {
                    // add a new row
                    ListRowStream lrs =
                        (ListRowStream)this.tables[i].getController()
                            .getRowStream();
                    lrs.getList().clear();
                    lrs.getList().addAll(rows);
                }
            }
        }
    }

    public void testM2MInSlicer() {
        System.out.println("Starting testM2MInSlicer");
        long start = System.currentTimeMillis();
        final String mdx =
            "Select\n"
            + "[Measures].[Count] on columns,\n"
            + "[Account].[All Accounts] on rows\n"
            + "From [M2M] WHERE {" + customerSet + "}\n";
        getTestContext().assertQueryReturns(
            mdx,
            "Axis #0:"
            + customerSetResult + "\n"
            + "Axis #1:\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Account].[All Accounts]}\n"
            + "Row #0: " + expectedCustomerSetSize + "\n");
        long stop = System.currentTimeMillis();
        System.out.println(
            "Time to execute (Ranges from 198ms to 257ms): "
            + (stop - start) + "ms");
    }

    public void testBasicM2MRollup() {
        System.out.println("Starting testBasicM2MRollup");
        long start = System.currentTimeMillis();
        NumberFormat nf = NumberFormat.getInstance();
        final String mdx =
            "Select\n"
            + "[Measures].[Count] on columns,\n"
            + "[Account].[All Accounts] on rows\n"
            + "From [M2M]\n";
        getTestContext().assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Count]}\n"
            + "Axis #2:\n"
            + "{[Account].[All Accounts]}\n"
            + "Row #0: " + nf.format(facts) + "\n");
        long stop = System.currentTimeMillis();
        System.out.println(
            "Time to execute (Ranges from 129ms to 138ms): "
            + (stop - start) + "ms");
    }
}
// End ManyToManyPerformanceTest.java