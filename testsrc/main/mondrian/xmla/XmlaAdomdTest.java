package mondrian.xmla;

import java.util.Properties;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;


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
        propSaver.set(
            MondrianProperties.instance().XmlaAlwaysIncludeDefaultSlicer, true);
        propSaver.set(
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension,
            true);
        propSaver.set(MondrianProperties.instance().XmlaUseMSSASError, true);
        propSaver.set(
            MondrianProperties.instance().XmlaCustomProviderVersion,
            "10.0.1600.22");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        getTestContext().flushSchemaCache();
    }

    protected void doExecuteTest() throws Exception {
        doTest(
            "EXECUTE",
            getDefaultRequestProperties("EXECUTE"),
            getTestContext() );
    }

    /**
     * Basic string adomd parameter.
     */
    public void testCommandParameter() throws Exception {
        doTest(
            "EXECUTE",
            getDefaultRequestProperties("EXECUTE"),
            getTestContext() );
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

    // TODO must test error types
}
