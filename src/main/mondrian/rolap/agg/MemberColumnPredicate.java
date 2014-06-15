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

import java.util.List;
import java.util.Map;

/**
 * Column constraint defined by a member.
 *
 * @author jhyde
 * @since Mar 16, 2006
 */
public class MemberColumnPredicate extends ValueColumnPredicate {
    private final RolapMember member;
    private final Map<String, SqlQuery> subqueryMap;
    /**
     * Creates a MemberColumnPredicate
     *
     * @param column Constrained column
     * @param member Member to constrain column to; must not be null
     */
    public MemberColumnPredicate(RolapStar.Column column, RolapMember member) {
        super(column, member.getKey());
        this.member = member;
        this.subqueryMap = null;
    }

    public MemberColumnPredicate(RolapStar.Column column, RolapMember member, Map<String, SqlQuery> subqueryMap) {
        super(column, member.getKey());
        this.member = member;
        this.subqueryMap = subqueryMap;
    }
    // for debug
    public String toString() {
        return member.getUniqueName();
    }

    public List<RolapStar.Column> getConstrainedColumnList() {
        return super.getConstrainedColumnList();
    }

    /**
     * Returns the <code>Member</code>.
     *
     * @return Returns the <code>Member</code>, not null.
     */
    public RolapMember getMember() {
        return member;
    }

    public boolean equals(Object other) {
        if (!(other instanceof MemberColumnPredicate)) {
            return false;
        }
        final MemberColumnPredicate that = (MemberColumnPredicate) other;
        return member.equals(that.getMember());
    }

    public int hashCode() {
        return member.hashCode();
    }

    public void describe(StringBuilder buf) {
        buf.append(member.getUniqueName());
    }

    public StarColumnPredicate cloneWithColumn(RolapStar.Column column) {
        return new MemberColumnPredicate(column, member);
    }

    public void toSql(SqlQuery sqlQuery, StringBuilder buf) {
        final RolapStar.Column column = getConstrainedColumn();
        if (subqueryMap != null && column.getTable() != null && column.getTable().getSubQueryAlias() != null) {
            // this will probably need to move into it's own separate "M2M Member" subclass.
            String expr = column.generateExprString(sqlQuery);

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
            query.subqueries.get(column.getTable().getSubQueryAlias()).addWhere(sb.toString());
            // TODO: If the dialect can't support the IN subquery scenario, then we can't
            // nativize.  We'll need a check in the Native layer for this.
            buf.append(") IN (");
            buf.append(query.subqueries.get(column.getTable().getSubQueryAlias()).toString());
            buf.append(")");
        } else {
            super.toSql(sqlQuery, buf);
        }
    }
}

// End MemberColumnPredicate.java
