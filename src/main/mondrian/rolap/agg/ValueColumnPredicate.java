/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.rolap.sql.SqlQuery;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A constraint which requires a column to have a particular value.
 *
 * @author jhyde
 * @since Nov 2, 2006
 */
public class ValueColumnPredicate
    extends AbstractColumnPredicate
    implements Comparable
{
    private final Object value;
    protected Map<String, SqlQuery> subqueryMap;


    public ValueColumnPredicate(
        RolapStar.Column constrainedColumn,
        Object value,
        Map<String, SqlQuery> subqueryMap)
    {
        this(constrainedColumn, value);
        this.subqueryMap = subqueryMap;
    }
    /**
     * Creates a column constraint.
     *
     * @param value Value to constraint the column to. (We require that it is
     *   {@link Comparable} because we will sort the values in order to
     *   generate deterministic SQL.)
     */
    public ValueColumnPredicate(
        RolapStar.Column constrainedColumn,
        Object value)
    {
        super(constrainedColumn);
//        assert constrainedColumn != null;
        assert value != null;
        assert ! (value instanceof StarColumnPredicate);
        this.value = value;
    }

    /**
     * The subquery map is used for many to many inline SQL query usecases.
     */
    public void setSubqueryMap(Map<String, SqlQuery> subqueryMap) {
      this.subqueryMap = subqueryMap;
    }

    /**
     * Returns the value which the column is compared to.
     */
    public Object getValue() {
        return value;
    }

    public String toString() {
        return String.valueOf(value);
    }

    public boolean equalConstraint(StarPredicate that) {
        return that instanceof ValueColumnPredicate
            && getConstrainedColumnBitKey().equals(
                that.getConstrainedColumnBitKey())
            && this.value.equals(((ValueColumnPredicate) that).value);
    }

    public int compareTo(Object o) {
        ValueColumnPredicate that = (ValueColumnPredicate) o;
        int columnBitKeyComp =
            getConstrainedColumnBitKey().compareTo(
                that.getConstrainedColumnBitKey());

        // First compare the column bitkeys.
        if (columnBitKeyComp != 0) {
            return columnBitKeyComp;
        }

        if (this.value instanceof Comparable
            && that.value instanceof Comparable
            && this.value.getClass() == that.value.getClass())
        {
            return ((Comparable) this.value).compareTo(that.value);
        } else {
            String thisComp = String.valueOf(this.value);
            String thatComp = String.valueOf(that.value);
            return thisComp.compareTo(thatComp);
        }
    }

    public boolean equals(Object other) {
        if (!(other instanceof ValueColumnPredicate)) {
            return false;
        }
        final ValueColumnPredicate that = (ValueColumnPredicate) other;

        // First compare the column bitkeys.
        if (!getConstrainedColumnBitKey().equals(
                that.getConstrainedColumnBitKey()))
        {
            return false;
        }

        if (value != null) {
            return value.equals(that.getValue());
        } else {
            return null == that.getValue();
        }
    }

    public int hashCode() {
        int hashCode = getConstrainedColumnBitKey().hashCode();

        if (value != null) {
            hashCode = hashCode ^ value.hashCode();
        }

        return hashCode;
    }

    public void values(Collection<Object> collection) {
        collection.add(value);
    }

    public boolean evaluate(Object value) {
        return this.value.equals(value);
    }

    public void describe(StringBuilder buf) {
        buf.append(value);
    }

    public Overlap intersect(StarColumnPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    public boolean mightIntersect(StarPredicate other) {
        return ((StarColumnPredicate) other).evaluate(value);
    }

    public StarColumnPredicate minus(StarPredicate predicate) {
        assert predicate != null;
        if (((StarColumnPredicate) predicate).evaluate(value)) {
            return LiteralStarPredicate.FALSE;
        } else {
            return this;
        }
    }

    public StarColumnPredicate cloneWithColumn(RolapStar.Column column) {
        return new ValueColumnPredicate(column, value);
    }

    public void toSql(SqlQuery sqlQuery, StringBuilder buf) {
        final RolapStar.Column column = getConstrainedColumn();
        String expr = column.generateExprString(sqlQuery);
        if (subqueryMap != null && column.getTable() != null && column.getTable().getSubQueryAlias() != null) {
            // this will probably need to move into it's own separate "M2M Member" subclass.

            // TODO: Support Multi-Level M2M, at the moment this assumes one level.  Need to push this up a layer,
            // probably implementing a ManyToManyColumnPredicate of some sort.  Some early code was added
            // to AndPredicate to start thinking about this scenario.

            StringBuilder sb = new StringBuilder();
            sb.append(expr);
            Object key = getValue();
            if (key == RolapUtil.sqlNullValue) {
                sb.append(" is null");
            } else {
                sb.append(" = ");
                sqlQuery.getDialect().quote(sb, key, column.getDatatype());
            }
            // The "sb" predicate needs added to the subquery, not the main one.  we need access 
            // to the foreign key where clause element to model this correctly
            SqlQuery query = subqueryMap.get(column.getTable().getSubQueryAlias());
            buf.append("(");
            List<String> keys = query.subwhereExpr.get(column.getTable().getSubQueryAlias());
            for (int i = 0; i < keys.size(); i++) {
                if (i != 0) {
                    buf.append(",");
                }
                buf.append(keys.get(i));
            }
            query.getSubQuery(column.getTable().getSubQueryAlias()).addWhere(sb.toString());

            // TODO: If the dialect can't support the IN subquery scenario, then we can't
            // nativize.  We'll need a check in the Native layer for this.
            buf.append(") IN (");
            buf.append(query.getSubQuery(column.getTable().getSubQueryAlias()).toString());
            buf.append(")");

            // remove the where clause just created so other predicates can apply their own
            // constraints.
            // TODO: support query.clone() instead of this approach.
            ((List)query.getSubQuery(column.getTable().getSubQueryAlias()).where).remove(sb.toString());
        } else {
            buf.append(expr);
            Object key = getValue();
            if (key == RolapUtil.sqlNullValue) {
                buf.append(" is null");
            } else {
                buf.append(" = ");
                sqlQuery.getDialect().quote(buf, key, column.getDatatype());
            }
        }
    }

    public BitKey checkInList(BitKey inListLHSBitKey) {
        // ValueColumn predicate by itself is not using IN list; when it is
        // one of the children to an OR predicate, then using IN list
        // is helpful. The later is checked by passing in a bitmap that
        // represent the LHS or the IN list, i.e. the column that is
        // constrained by the OR.
        BitKey inListRHSBitKey = inListLHSBitKey.copy();

        if (!getConstrainedColumnBitKey().equals(inListLHSBitKey)
            || value == RolapUtil.sqlNullValue)
        {
            inListRHSBitKey.clear();
        }

        return inListRHSBitKey;
    }

    public void toInListSql(SqlQuery sqlQuery, StringBuilder buf) {
        sqlQuery.getDialect().quote(
            buf, value, getConstrainedColumn().getDatatype());
    }
}

// End ValueColumnPredicate.java
