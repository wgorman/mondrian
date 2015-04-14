/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2004-2005 TONBELLER AG
 * Copyright (C) 2005-2005 Julian Hyde
 * Copyright (C) 2005-2015 Pentaho and others
 * All Rights Reserved.
 */

package mondrian.rolap;

import mondrian.olap.MondrianDef;

import java.util.*;

/**
 * Represents (RolapCube)Members that have been matched across
 * constraints as being non-null, non-all, not
 * calculated and of the same level within the same cube.The only difference is their identity.
 * The matched member is the member belonging to the constraint that owns this level member.
 * The list of members are those defined in other constraints.
 */
public class ConsolidationMembers {

    private MondrianDef.Expression levelExpression;
    private boolean crossJoin;
    private boolean descendantCrossJoin;
    private String columnExpression;
    private RolapMember matchedMember;
    private List<RolapMember> members = new ArrayList<RolapMember>();
    private RolapStar.Column starColumn;

    public ConsolidationMembers(RolapMember matchedMember, MondrianDef.Expression levelExpression,
                                boolean crossjoin) {
        this.matchedMember = matchedMember;
        this.levelExpression = levelExpression;
        this.crossJoin = crossjoin;
    }

    public ConsolidationMembers(RolapMember matchedMember, MondrianDef.Expression levelExpression,
                                boolean crossjoin, boolean descendantCrossJoin) {
        this.matchedMember = matchedMember;
        this.levelExpression = levelExpression;
        this.crossJoin = crossjoin;
        this.descendantCrossJoin = descendantCrossJoin;
    }

    public ConsolidationMembers(RolapMember matchedMember, MondrianDef.Expression levelExpression) {
        this(matchedMember, levelExpression, false);
    }

    public RolapMember getMatchedMember() {
        return matchedMember;
    }

    public boolean isCrossJoin() {
        return crossJoin;
    }

    public boolean isDescendantCrossJoin() {
        return descendantCrossJoin;
    }

    /**
     * The expression that defines the Rolap Level.
     * @return
     */
    public MondrianDef.Expression getLevelExpression() {
        return levelExpression;
    }

    public String getColumnExpression() {
        return columnExpression;
    }

    /**
     * returns just the members matched from other constraints.
     *
     * @return
     */
    public List<RolapMember> getMembers() {
        return new ArrayList<RolapMember>(members);
    }

    /**
     * Returns all members including the local matched one.
     *
     * @return
     */
    public List<RolapMember> getAllMembers() {
        ArrayList<RolapMember> list = new ArrayList<RolapMember>(members);
        list.add(0, matchedMember);
        return list;
    }

    /**
     * returns the number of members from other constraints.
     *
     * @return
     */
    public int getMemberCount() {
        return members.size();
    }

    /**
     * Set the column expression in SQL terms.
     * @param columnExpression
     */
    public void setColumnExpression(String columnExpression) {
        this.columnExpression = columnExpression;
    }

    public RolapStar.Column getStarColumn() {
        return starColumn;
    }

    public void setStarColumn(RolapStar.Column starColumn) {
        this.starColumn = starColumn;
    }

    // we use a list to presever order, but still want unique items
    public void addMember(RolapMember member) {
        if (!members.contains(member)) {
            this.members.add(member);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LevelMembers{");
        sb.append("matchedMember=").append(matchedMember);
        sb.append(", levelExpression=").append(levelExpression);
        sb.append(", crossJoin=").append(crossJoin);
        sb.append(", descendantCrossJoin=").append(descendantCrossJoin);
        sb.append(", columnExpression='").append(columnExpression).append('\'');
        sb.append(", members=").append(members);
        sb.append('}');
        return sb.toString();
    }
}
