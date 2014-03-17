/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2014 Pentaho
// All rights reserved.
*/
package mondrian.xmla;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

import java.util.Properties;

/**
 * ADOMD.NET compliance tests and related MS SSAS compatibility
 */
public class XmlaAdomdTest extends XmlaBaseTestCase {

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaAdomdTest.class);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return null;
    }

    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }

    protected void setUp() throws Exception {
        super.setUp();
        // flush schema to avoid problems with ssasCompatibility change
        // running test suites
        getTestContext().flushSchemaCache();
        propSaver.set(MondrianProperties.instance().SsasCompatibleNaming, true);
        // avoids parse errors on adomd when no slicer defined
        propSaver.set(
            MondrianProperties.instance().XmlaAlwaysIncludeDefaultSlicer, true);
        propSaver.set(
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension,
            true);
        // using ms format avoids parse errors on adomd when returning error
        propSaver.set(MondrianProperties.instance().XmlaUseMSSASError, true);
        // some properties always throw on adomd if version isn't recognized
        propSaver.set(
            MondrianProperties.instance().XmlaCustomProviderVersion,
            "10.0.1600.22");
        // default hierarchy key lookup work the same as name lookups, like ssas
        propSaver.set(MondrianProperties.instance().SsasKeyLookup, true);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        // avoid side effects from ssas naming
        getTestContext().flushSchemaCache();
    }

    /**
     * Basic string adomd parameter.
     */
    public void testCommandParameter() throws Exception {
        doTest(
            "EXECUTE",
            getDefaultRequestProperties("EXECUTE"),
            getTestContext());
    }

    /**
     * Issue where a shared dimension hierarchy would cause two HierarchyInfo
     * elements with the same name to appear in AxisInfo
     * @throws Exception
     */
    public void testDuplicateHierarchyInSlicer() throws Exception {
        String storeStoreDim =
            "  <Dimension type=\"StandardDimension\" visible=\"true\" name=\"SharedStore\">\n"
            + "    <Hierarchy name=\"SharedStore\" hasAll=\"true\" allMemberName=\"All\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>";
        String duplicateHierarchyCube =
            "<Cube name=\"StoreDup\">\n"
            + "  <Table name=\"inventory_fact_1997\"/>\n"
            + "  <DimensionUsage source=\"SharedStore\" name=\"A Store\" visible=\"true\" foreignKey=\"region_id\"/>\n"
            + "  <DimensionUsage source=\"SharedStore\" name=\"AnotherStore\" visible=\"true\" foreignKey=\"employee_id\"/>\n"
            + "<DimensionUsage name=\"Warehouse\" source=\"Warehouse\" foreignKey=\"warehouse_id\"/>"
            + "<Measure name=\"Store Invoice\" column=\"store_invoice\" aggregator=\"sum\"/>"
            + "</Cube>";
        TestContext testContext = TestContext.instance().create(
            storeStoreDim,
            duplicateHierarchyCube,
            null,
            null,
            null,
            null);

        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, testContext);
    }

    /**
     * Have CoalesceEmpty treat empty strings as null
     * @throws Exception
     */
    public void testCoalesceEmptyEmptyString() throws Exception {
        Result result = executeQuery(
            "with\n"
            + "    member Measures.[EmptyStr] as 'coalesceempty(\"\",\"NotEmpty\")'\n"
            + "select \n"
            + "    {Measures.[EmptyStr]} on columns,\n"
            + "    {[Store].[All Stores].[USA].[WA]} on rows\n"
            + "from \n"
            + "    [Sales]");
        Object value = result.getCell(new int[]{0, 0}).getValue();
        assertEquals(
            "CoalesceEmpty failed to replace empty string", "NotEmpty", value);
    }

    // TODO: MOVE TO Ssas2005 and review commented tests
    /**
     * Children lookup by key and default
     * @throws Exception
     */
    public void testSsasKeyLookup() throws Exception {
        final String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[CA].[Berkeley].[Judith Frazier]}\n"
            + "Row #0: \n";

        // test child by key, name != key
        assertQueryReturns(
            "SELECT\n"
            + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Customers].[USA].[CA].[Berkeley].&[371]} ON ROWS\n"
            + "FROM [Sales]",
            result);

        // use keys at every level
        assertQueryReturns(
            "SELECT\n"
            + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Customers].&[USA].&[CA].&[Berkeley].&[371]} ON ROWS\n"
            + "FROM [Sales]",
            result);

        // test mixed name/key lookup
        assertQueryReturns(
            "SELECT\n"
            + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Customers].[USA].&[CA].[Berkeley].&[371]} ON ROWS\n"
            + "FROM [Sales]",
            result);

        // normal level lookup
        assertQueryReturns(
            "SELECT\n"
            + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Customers].[Name].&[371]} ON ROWS\n"
            + "FROM [Sales]",
            result);
    }

    /**
     * Test SSAS-style key access with native mode disabled.
     * @throws Exception
     */
    public void testSsasKeyNoNative() throws Exception {
        final boolean nativeNonEmpty =
            MondrianProperties.instance().EnableNativeNonEmpty.get();
        try {
            propSaver.set(
                MondrianProperties.instance().EnableNativeNonEmpty, false);
            assertQueryReturns(
                "SELECT\n"
                + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
                + "{[Customers].[USA].&[CA].[Berkeley].&[371]} ON ROWS\n"
                + "FROM [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Unit Sales]}\n"
                + "Axis #2:\n"
                + "{[Customers].[USA].[CA].[Berkeley].[Judith Frazier]}\n"
                + "Row #0: \n");
        } finally {
            propSaver.set(
                MondrianProperties.instance()
                    .EnableNativeNonEmpty, nativeNonEmpty);
        }
    }

    /**
     * Level compound key access and child key lookup
     * @throws Exception
     */
    public void testCompoundKeyAccessChildKeyLookup() throws Exception {
        final String result =
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Customers].[USA].[CA].[Berkeley].[Judith Frazier]}\n"
            + "Row #0: \n";

        // child by name after compound key
        assertQueryReturns(
            "SELECT\n"
            + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Customers].[City].&[Berkeley]&[CA].[Judith Frazier]} ON ROWS\n"
            + "FROM [Sales]",
            result);

        // child by key after compound key
        assertQueryReturns(
            "SELECT\n"
            + "{[Measures].[Unit Sales]} ON COLUMNS,\n"
            + "{[Customers].[City].&[Berkeley]&[CA].&[371]} ON ROWS\n"
            + "FROM [Sales]",
            result);
    }

    public void testKeyLookupStrToMember() {
        assertAxisReturns(
            "StrToMember('[Customers].[USA].[CA].&[Berkeley]')",
            "[Customers].[USA].[CA].[Berkeley]");
        assertAxisReturns(
            "StrToMember('[Customers].&[USA].&[CA].&[Berkeley]')",
            "[Customers].[USA].[CA].[Berkeley]");
        assertAxisReturns(
            "StrToMember('[Customers].[City].&[Berkeley]&[CA].&[371]')",
            "[Customers].[USA].[CA].[Berkeley].[Judith Frazier]");
    }

}
// End XmlaAdomdTest.java