/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.olap.MondrianProperties;
import mondrian.test.TestContext;
import mondrian.test.loader.CsvDBTestCase;

public class LinkMemberTest extends CsvDBTestCase {

    private static final String DIRECTORY =
        "testsrc/main/mondrian/olap/fun/csv";

    private static final String FILENAME = "link_member.csv";

    private static final String TimeWeekly =
        MondrianProperties.instance().SsasCompatibleNaming.get()
            ? "[Time].[Weekly]"
            : "[Time.Weekly]";

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
        return TestContext.instance().withSchema(
            "<?xml version=\"1.0\"?>\n"
            + "<Schema name=\"FoodMart\">\n"
            + "  <Dimension name=\"Store\">\n"
            + "    <Hierarchy hasAll=\"true\" primaryKey=\"store_id\">\n"
            + "      <Table name=\"store\"/>\n"
            + "      <Level name=\"Store Country\" column=\"store_country\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store State\" column=\"store_state\" uniqueMembers=\"true\"/>\n"
            + "      <Level name=\"Store City\" column=\"store_city\" uniqueMembers=\"false\"/>\n"
            + "      <Level name=\"Store Name\" column=\"store_name\" uniqueMembers=\"true\">\n"
            + "        <Property name=\"Store Type\" column=\"store_type\"/>\n"
            + "        <Property name=\"Store Manager\" column=\"store_manager\"/>\n"
            + "        <Property name=\"Store Sqft\" column=\"store_sqft\" type=\"Numeric\"/>\n"
            + "        <Property name=\"Grocery Sqft\" column=\"grocery_sqft\" type=\"Numeric\"/>\n"
            + "        <Property name=\"Frozen Sqft\" column=\"frozen_sqft\" type=\"Numeric\"/>\n"
            + "        <Property name=\"Meat Sqft\" column=\"meat_sqft\" type=\"Numeric\"/>\n"
            + "        <Property name=\"Has coffee bar\" column=\"coffee_bar\" type=\"Boolean\"/>\n"
            + "        <Property name=\"Street address\" column=\"store_street_address\" type=\"String\"/>\n"
            + "      </Level>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Dimension name=\"Time2\" type=\"TimeDimension\">\n"
            + "    <Hierarchy primaryKey=\"time_id\" hasAll=\"true\">\n"
            + "      <Table name=\"time_by_day_2\"/>\n"
            + "      <Level name=\"Quarter\" uniqueMembers=\"false\" column=\"quarter\" levelType=\"TimeQuarters\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "  <Dimension name=\"QuarterPeriods\" type=\"StandardDimension\">\n"
            + "    <Hierarchy primaryKey=\"quarter_id\" hasAll=\"true\">\n"
            + "      <Table name=\"quarter_table\"/>\n"
            + "      <Level name=\"Quarter4\" column=\"quarter_4\" uniqueMembers=\"true\"\n"
            + "          levelType=\"Regular\" hideMemberIf=\"Never\"/>\n"
            + "      <Level name=\"Quarter3\" column=\"quarter_3\" uniqueMembers=\"true\"\n"
            + "          levelType=\"Regular\" hideMemberIf=\"IfBlankName\"/>"
            + "      <Level name=\"Quarter2\" column=\"quarter_2\" uniqueMembers=\"true\"\n"
            + "          levelType=\"Regular\" hideMemberIf=\"IfBlankName\"/>"
            + "      <Level name=\"Quarter1\" column=\"quarter_1\" uniqueMembers=\"true\"\n"
            + "          levelType=\"Regular\" hideMemberIf=\"IfBlankName\"/>\n"
            + "      <Level name=\"RefIdQuarter\" column=\"quarter_id\" uniqueMembers=\"true\"\n"
            + "          levelType=\"Regular\" hideMemberIf=\"IfBlankName\"/>"
            + "    </Hierarchy>\n"
            + "  </Dimension>\n"
            + "\n"
            + "<Dimension name=\"Time\" type=\"TimeDimension\">\n"
            + "    <Hierarchy hasAll=\"false\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Quarter\" column=\"quarter\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeQuarters\"/>\n"
            + "      <Level name=\"Month\" column=\"month_of_year\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeMonths\"/>\n"
            + "    </Hierarchy>\n"
            + "    <Hierarchy hasAll=\"true\" name=\"Weekly\" primaryKey=\"time_id\">\n"
            + "      <Table name=\"time_by_day\"/>\n"
            + "      <Level name=\"Year\" column=\"the_year\" type=\"Numeric\" uniqueMembers=\"true\"\n"
            + "          levelType=\"TimeYears\"/>\n"
            + "      <Level name=\"Week\" column=\"week_of_year\" type=\"Numeric\" uniqueMembers=\"false\"\n"
            + "          levelType=\"TimeWeeks\"/>\n"
            + "      <Level name=\"Day\" column=\"day_of_month\" uniqueMembers=\"false\" type=\"Numeric\"\n"
            + "          levelType=\"TimeDays\"/>\n"
            + "    </Hierarchy>\n"
            + "  </Dimension>"
            + "\n"
            + "  <Cube name=\"SalesCube\">\n"
            + "    <Table name=\"quarter_sales_fact_1997\"/>\n"
            + "    <DimensionUsage name=\"Store\" source=\"Store\" foreignKey=\"store_id\"/>\n"
            + "    <DimensionUsage name=\"Time\" source=\"Time\" foreignKey=\"time_id\"/>\n"
            + "    <DimensionUsage name=\"Time2\" source=\"Time2\" foreignKey=\"time_id\"/>\n"
            + "    <DimensionUsage name=\"QuarterPeriods\" source=\"QuarterPeriods\" foreignKey=\"quarter_id\"/>\n"
            + "  </Cube>\n"
            + "</Schema>\n");
    }

    public void testLinkMember() throws Exception {
        // apart from weekly having an all member,
        // time and weekly hierarchies are equivalent up to year
        TestContext context = createTestContext();
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997], " + TimeWeekly + ")",
            "[Time].[Weekly].[1997]");
    }

    public void testLinkMemberAll() throws Exception {
        TestContext context = createTestContext();
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time2].[All Time2s], [QuarterPeriods])",
            "[QuarterPeriods].[All QuarterPeriodss]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Store].[All Stores], [QuarterPeriods])",
            "[QuarterPeriods].[All QuarterPeriodss]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([QuarterPeriods].[All QuarterPeriodss], [Store])",
            "[Store].[All Stores]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([QuarterPeriods].[All QuarterPeriodss], [Time])",
            "");
    }

    public void testLinkMemberDims() throws Exception {
        String doubleTimeCube =
            "<Cube name=\"SalesTime\">\n"
            + "  <Table name=\"sales_fact_1997\"/>\n"
            + "  <DimensionUsage source=\"Time\" name=\"Time\" visible=\"true\" foreignKey=\"time_id\"/>\n"
            + "  <DimensionUsage source=\"Time\" name=\"SecondTime\" visible=\"true\" foreignKey=\"time_id\"/>\n"
            + "  <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\"/>"
            + "</Cube>";
        TestContext testContext = getTestContext().create(
            null,
            doubleTimeCube,
            null,
            null,
            null,
            null);
        testContext.withCube("SalesTime").assertAxisReturns(
            "LinkMember([Time].[1997].[Q1].[2], [SecondTime])",
            "[SecondTime].[1997].[Q1].[2]");
    }

    public void testLinkMemberType() {
        // ensure it declares the right hierarchy return type
        TestContext context = createTestContext();
        context.withCube("SalesCube").assertAxisReturns(
            "CrossJoin([Time].[1997], LinkMember([Time].[1997], "
            + TimeWeekly + "))",
            "{[Time].[1997], [Time].[Weekly].[1997]}");
    }

    public void testLinkMemberWithRaggedHierarchies() {
        TestContext context = createTestContext();
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time2].[Q1], [QuarterPeriods])",
            "[QuarterPeriods].[null].[null].[null].[Q1]");

        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time2].[Q2], [QuarterPeriods])",
            "[QuarterPeriods].[null].[null].[Q2]");

        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time2].[Q3], [QuarterPeriods])",
            "[QuarterPeriods].[null].[Q3]");

        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time2].[Q4], [QuarterPeriods])",
            "[QuarterPeriods].[Q4]");
    }

    public void testLinkMemberWithRaggedHierarchiesEmptyResult() {
        TestContext context = createTestContext();
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997], [QuarterPeriods])",
            "");
    }

    public void testLinkMemberWithRaggedHierarchiesOnTimeDimension() {
        //Null pointer exception occurs
        TestContext context = createTestContext();
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q1], [QuarterPeriods])",
            "[QuarterPeriods].[null].[null].[null].[Q1]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q2], [QuarterPeriods])",
            "[QuarterPeriods].[null].[null].[Q2]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q3], [QuarterPeriods])",
            "[QuarterPeriods].[null].[Q3]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q4], [QuarterPeriods])",
            "[QuarterPeriods].[Q4]");
    }

    public void testLinkMemberOnTimeDimension() {
        //Null pointer exception occurs
        TestContext context = createTestContext();
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q1], [Time2])",
            "[Time2].[Q1]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q2], [Time2])",
            "[Time2].[Q2]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q3], [Time2])",
            "[Time2].[Q3]");
        context.withCube("SalesCube").assertAxisReturns(
            "LinkMember([Time].[1997].[Q4], [Time2])",
            "[Time2].[Q4]");
    }
}
// End LinkMemberTest.java
