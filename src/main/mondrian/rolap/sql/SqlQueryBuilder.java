/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.Util;
import mondrian.rolap.*;
import mondrian.spi.Dialect;
import mondrian.util.Pair;

import java.util.*;

/**
* Model for building SQL query.
 *
 * <p>Higher level than {@link SqlQuery}.</p>
 */
public class SqlQueryBuilder {
    public final SqlQuery sqlQuery;
    public final SqlTupleReader.ColumnLayoutBuilder layoutBuilder;
    private final BitSet orderBitSet = new BitSet();
    private final Set<Pair<Table, Joiner>> fromList =
        new LinkedHashSet<Pair<Table, Joiner>>();

    /** Fact table. If set, we join all other tables used in the query to the
     * fact table even if no measures are used. */
    public RolapMeasureGroup fact;

    /** Whether to join to the inner table of a star dimension if an attribute
     * from any table in the dimension is used. For example, if we use
     * product_class.product_department we would join to the product table. This
     * ensures that members without children are omitted. */
    public boolean joinToDimensionKey;

    private final Map<TableKey, Table> tableMap =
        new LinkedHashMap<TableKey, Table>();
    private Map<RolapStar.Table, Table> tableMap2 =
        new LinkedHashMap<RolapStar.Table, Table>();
    private Set<Table> tableSet = new LinkedHashSet<Table>();

    /**
     * Creates a SqlQueryBuilder.
     *
     * @param sqlQuery SQL query
     * @param layoutBuilder Column layout builder
     * @param keyList Key of starting point for query; other attributes
     *   will be joined to this
     */
    @Deprecated
    public SqlQueryBuilder(
        SqlQuery sqlQuery,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder,
        List<RolapSchema.PhysColumn> keyList)
    {
        this(sqlQuery, layoutBuilder);
        addListToFrom(keyList, AutoJoiner.INSTANCE);
    }

    /**
     * Creates a SqlQueryBuilder.
     *
     * @param dialect Dialect
     * @param err Description of purpose of query
     * @param layoutBuilder Column layout builder
     */
    public SqlQueryBuilder(
        Dialect dialect,
        String err,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        this(new ProtectedSqlQuery(dialect, err), layoutBuilder);
    }

    /**
     * Creates a SqlQueryBuilder.
     *
     * @param sqlQuery SQL query
     * @param layoutBuilder Column layout builder
     */
    public SqlQueryBuilder(
        SqlQuery sqlQuery,
        SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
    {
        this.sqlQuery = sqlQuery;
        this.layoutBuilder = layoutBuilder;
    }

    public final void addListToFrom(
        List<? extends RolapSchema.PhysExpr> exprList,
        Joiner joiner)
    {
        for (RolapSchema.PhysExpr expr : exprList) {
            addToFrom(expr, joiner);
        }
    }

    public void addToFrom(RolapSchema.PhysExpr expr, Joiner joiner) {
        assert false; // TODO: obsolete calls to this method
        expr.foreachColumn(this, joiner);
    }


    public void addRelation(Table table, Joiner joiner) {
        fromList.add(Pair.of(table, joiner));
    }

    public boolean addRelation(
        RolapSchema.PhysRelation relation, Joiner joiner)
    {
        Util.deprecated("TODO: obsolete calls to this method", true);
        joiner.addRelation(this, relation);
        return true;
    }

    void addRelation(
        RolapSchema.PhysRelation relation,
        String parentAlias,
        String joinCondition)
    {
        sqlQuery.addFrom(
            relation,
            relation.getAlias(),
            parentAlias,
            joinCondition,
            false);
    }

    public final Dialect getDialect() {
        return sqlQuery.getDialect();
    }

    // creates a Column
    public Column column(
        RolapSchema.PhysColumn column, RolapCubeDimension dimension)
    {
        Table table = table(column.relation, dimension);
        return new Column(table, column);
    }

    /** Creates a column from a star table. */
    public Column column(
        RolapSchema.PhysColumn column,
        RolapStar.Table starTable)
    {
        Table table = table(starTable);
        return new Column(table, column);
    }

    private Table table(RolapStar.Table starTable) {
        Table table = tableMap2.get(starTable);
        if (table == null) {
            final Pair<Table,RolapSchema.PhysLink> parent =
                parentTable(starTable);
            if (parent == null) {
                table = new Table(null, null, starTable.getRelation(), null);
            } else {
                table =
                    new Table(
                        parent.left, parent.right, starTable.getRelation(),
                        null);
            }
            tableMap2.put(starTable, table);
            tableSet.add(table);
        }
        return table;
    }

    private Pair<Table,RolapSchema.PhysLink> parentTable(
        RolapStar.Table starTable)
    {
        if (starTable.getParentTable() == null) {
            return null;
        }
        Table table = table(starTable.getParentTable());
        final RolapSchema.PhysLink link =
            Util.last(starTable.getPath().hopList).link;
        return Pair.of(table, link);
    }

    public Table table(
        RolapSchema.PhysRelation relation,
        RolapCubeDimension dimension)
    {
        // TODO: if two dimensions cause the same join path from the fact table
        // then we should return the same table. Good enough for 99% of
        // schemas/queries right now, though.
        TableKey key = new TableKey(relation, dimension);
        Table table = tableMap.get(key);
        if (table == null) {
            final Pair<Table, RolapSchema.PhysLink> parent =
                parentTable(relation, dimension);
            if (parent == null) {
                table = new Table(null, null, relation, dimension);
            } else {
                table =
                    new Table(parent.left, parent.right, relation, dimension);
            }
            tableMap.put(key, table);
            tableSet.add(table);
        }
        return table;
    }

    private Pair<Table, RolapSchema.PhysLink> parentTable(
        RolapSchema.PhysRelation physRelation, RolapCubeDimension dimension)
    {
        if (dimension == null) {
            assert physRelation == fact.getFactRelation();
            return null;
        }
        final RolapSchema.PhysPath path;
        RolapSchema.PhysRelation keyPhysRelation = dimension.getKeyTable();
        if (physRelation == keyPhysRelation) {
            if (fact == null) {
                return null;
            } else {
                path = fact.dimensionMap3.get(dimension);
                final RolapSchema.PhysLink link = path.getLinks().get(0);
                Table table = table(link.targetRelation, null);
                return Pair.of(table, link);
            }
        } else {
            final RolapSchema.PhysSchemaGraph graph =
                physRelation.getSchema().getGraph();
            try {
                path =
                    graph.findPath(
                        physRelation, Collections.singleton(keyPhysRelation),
                        false);
            } catch (RolapSchema.PhysSchemaException e) {
                // TODO: pre-compute path for all attributes, so this
                // error could never be the result of a user error in
                // the schema definition
                Util.deprecated("TODO", false);
                throw Util.newInternal(
                    e, "while finding path from attribute to dimension key");
            }
            final RolapSchema.PhysLink link = path.getLinks().get(0);
            Table table = table(link.targetRelation, dimension);
            return Pair.of(table, link);
        }
    }

    /** Short-hand for calls to {@link #column} and {@link #addColumn}. */
    public void addColumns(
        Iterable<? extends RolapSchema.PhysColumn> columns,
        RolapCubeDimension dimension,
        Clause clause,
        Joiner joiner)
    {
        for (RolapSchema.PhysColumn physColumn : columns) {
            addColumn(column(physColumn, dimension), clause, joiner);
        }
    }

    public int addColumn(Column column, Clause clause) {
        return addColumn(column, clause, NullJoiner.INSTANCE);
    }

    public int addColumn(Column column, Clause clause, Joiner joiner) {
        if (column == null) {
            return -1;
        }
        final String expString = column.sql;
        int ordinal = layoutBuilder.lookup(expString);
        if (ordinal >= 0) {
            switch (clause) {
            case SELECT_GROUP_ORDER:
            case SELECT_ORDER:
                if (!orderBitSet.get(ordinal)) {
                    sqlQuery.addOrderBy(expString, true, false, true);
                    orderBitSet.set(ordinal);
                }
            }
            return ordinal;
        }
        addToFrom(column.table, joiner);
        final String alias;
        switch (clause) {
        case SELECT:
            alias = sqlQuery.addSelect(
                expString, column.physColumn.getInternalType());
            break;
        case SELECT_GROUP:
            alias = sqlQuery.addSelectGroupBy(
                expString, column.physColumn.getInternalType());
            break;
        case SELECT_ORDER:
            sqlQuery.addOrderBy(expString, true, false, true);
            alias = sqlQuery.addSelect(
                expString, column.physColumn.getInternalType());
            break;
        case SELECT_GROUP_ORDER:
            sqlQuery.addOrderBy(expString, true, false, true);
            alias = sqlQuery.addSelectGroupBy(
                expString, column.physColumn.getInternalType());
            break;
        case FROM:
            return -1;
        default:
            throw Util.unexpected(clause);
        }
        ordinal = layoutBuilder.register(expString, alias);
        switch (clause) {
        case SELECT_GROUP_ORDER:
        case SELECT_ORDER:
            sqlQuery.addOrderBy(expString, true, false, true);
            orderBitSet.set(ordinal);
        }
        return ordinal;
    }

    private void addToFrom(Table table, Joiner joiner) {
        fromList.add(Pair.of(table, joiner));
    }

    /** Sends expressions to the underlying query. */
    private void flush() {
        if (joinToDimensionKey) {
            for (Pair<Table, Joiner> pair
                : new ArrayList<Pair<Table, Joiner>>(fromList))
            {
                final RolapCubeDimension dimension = pair.left.dimension;
                if (dimension != null) {
                    addToFrom(
                        table(dimension.getKeyTable(), dimension),
                        NullJoiner.INSTANCE);
                }
            }
        }
        final Set<Table> added = new HashSet<Table>();
        for (Pair<Table, Joiner> pair : fromList) {
            addRecursive(pair.left, added);
        }
    }

    /** Adds a table to the query, if it has already been added. Always adds
     * a node's parent before the node. Therefore the FROM clause is always
     * in hierarchical order. For example, SALES_FACT_1997 comes before PRODUCT
     * comes before PRODUCT_CLASS. TIME_BY_DAY may come before or after PRODUCT.
     *
     * @param table Table to add
     * @param added Set of tables that have already been added
     */
    private void addRecursive(Table table, Set<Table> added) {
        if (added.contains(table)) {
            return;
        }
        if (table.parent != null) {
            addRecursive(table.parent, added);
            ((ProtectedSqlQuery) sqlQuery).addFromSuper(
                table.physRelation, table.physRelation.getAlias(),
                table.parent.physRelation.getAlias(), table.link.sql,
                true);
        } else {
            ((ProtectedSqlQuery) sqlQuery).addFromSuper(
                table.physRelation, table.physRelation.getAlias(), true);
        }
        added.add(table);
    }

    public Pair<String, List<SqlStatement.Type>> toSqlAndTypes() {
        flush();
        return ((ProtectedSqlQuery) sqlQuery).toSqlAndTypesSuper();
    }

    public interface Joiner {
        void addColumn(
            SqlQueryBuilder queryBuilder,
            RolapSchema.PhysColumn column);
        void addRelation(
            SqlQueryBuilder queryBuilder,
            RolapSchema.PhysRelation relation);
    }

    public static class AutoJoiner {
        public static final Joiner INSTANCE = NullJoiner.INSTANCE;
    }

    /** Joiner that joins a column to the key attribute of a dimension, and that
     * dimension to the fact table of a measure group. */
    public static class DimensionJoiner implements Joiner {
        private final RolapMeasureGroup measureGroup;
        private final RolapCubeDimension dimension;

        public DimensionJoiner(
            RolapMeasureGroup measureGroup,
            RolapCubeDimension dimension)
        {
            this.measureGroup = measureGroup;
            this.dimension = dimension;
        }

        public void addColumn(
            SqlQueryBuilder queryBuilder,
            RolapSchema.PhysColumn column)
        {
            queryBuilder.addRelation(column.relation, this);
        }

        public void addRelation(
            SqlQueryBuilder queryBuilder, RolapSchema.PhysRelation relation)
        {
            queryBuilder.sqlQuery.addFrom(
                relation,
                relation.getAlias(),
                false);
            final RolapSchema.PhysPath path =
                measureGroup.getPath(dimension);
            for (RolapSchema.PhysHop hop : path.hopList) {
                String joinCondition;
                String parentAlias;
                if (hop.link != null) {
                    joinCondition = hop.link.sql;
                    parentAlias = hop.link.targetRelation.getAlias();
                } else {
                    joinCondition = null;
                    parentAlias = null;
                }
                queryBuilder.addRelation(
                    hop.relation, parentAlias, joinCondition);
            }
        }

        public static Joiner of(
            RolapMeasureGroup measureGroup, RolapCubeDimension dimension)
        {
            if (measureGroup == null) {
                return AutoJoiner.INSTANCE;
            } else {
                return new DimensionJoiner(measureGroup, dimension);
            }
        }
    }

    public static class NullJoiner implements Joiner {
        public static final NullJoiner INSTANCE = new NullJoiner();

        private NullJoiner() {
        }

        public void addColumn(
            SqlQueryBuilder queryBuilder,
            RolapSchema.PhysColumn column)
        {
            queryBuilder.addRelation(column.relation, this);
        }

        public void addRelation(
            SqlQueryBuilder queryBuilder, RolapSchema.PhysRelation relation)
        {
            queryBuilder.sqlQuery.addFrom(
                relation,
                relation.getAlias(),
                false);
        }
    }

    /** Column in this query. It knows its join path to all tables in this
     * query. */
    public static class Column {
        private final Table table;
        private final RolapSchema.PhysColumn physColumn;
        private final String sql;

        private Column(Table table, RolapSchema.PhysColumn physColumn) {
            assert table != null;
            this.table = table;
            this.physColumn = physColumn;
            this.sql = physColumn.toSql();
        }
    }

    /** Table in this query. */
    public static class Table {
        private final RolapSchema.PhysLink link;
        private final RolapSchema.PhysRelation physRelation;
        private final RolapCubeDimension dimension; //optional
        private final RolapSchema.PhysPath path;
        private final Table parent; // optional

        private Table(
            Table parent,
            RolapSchema.PhysLink link,
            RolapSchema.PhysRelation physRelation,
            RolapCubeDimension dimension)
        {
            this.parent = parent;
            this.link = link;
            this.physRelation = physRelation;
            this.dimension = dimension;
            if (parent == null) {
                this.path =
                    new RolapSchema.PhysPathBuilder(physRelation).done();
            } else {
                this.path =
                    new RolapSchema.PhysPathBuilder(parent.path)
                        .prepend(link, physRelation, false)
                        .done();
            }
        }

        @Override
        public String toString() {
            return physRelation.toString();
        }

        @Override
        public int hashCode() {
            return Util.hashV(0, physRelation, parent, link);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                || obj instanceof Table
                && Util.equals(physRelation, ((Table) obj).physRelation)
                && Util.equals(parent, ((Table) obj).parent)
                && Util.equals(link, ((Table) obj).link);
        }
    }

    private static class TableKey
        extends Pair<RolapSchema.PhysRelation, RolapCubeDimension>
    {
        private TableKey(
            RolapSchema.PhysRelation physRelation, RolapCubeDimension dimension)
        {
            super(physRelation, dimension);
        }
    }

    /** Query that is inside a {@link mondrian.rolap.sql.SqlQueryBuilder}
     * and whose methods are protected from being called directly. Use the
     * methods in SqlQueryBuilder instead. */
    private static class ProtectedSqlQuery extends SqlQuery {
        public ProtectedSqlQuery(Dialect dialect, String err) {
            super(dialect);
        }

        @Override
        public Pair<String, List<SqlStatement.Type>> toSqlAndTypes() {
            throw new AssertionError();
        }

        private Pair<String, List<SqlStatement.Type>> toSqlAndTypesSuper() {
            return super.toSqlAndTypes();
        }

        @Override
        public boolean addFrom(
            RolapSchema.PhysRelation relation,
            String alias,
            boolean failIfExists)
        {
            throw new AssertionError();
        }

        private boolean addFromSuper(
            RolapSchema.PhysRelation relation,
            String alias,
            boolean failIfExists)
        {
            return super.addFrom(relation, alias, failIfExists);
        }

        @Override
        public boolean addFrom(
            RolapSchema.PhysRelation relation, String alias, String parentAlias,
            String joinCondition, boolean failIfExists)
        {
            throw new AssertionError();
        }

        public boolean addFromSuper(
            RolapSchema.PhysRelation relation, String alias, String parentAlias,
            String joinCondition, boolean failIfExists)
        {
            return super.addFrom(
                relation, alias, parentAlias, joinCondition, failIfExists);
        }
    }
}

// End SqlQueryBuilder.java
