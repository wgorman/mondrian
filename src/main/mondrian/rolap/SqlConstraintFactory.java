/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.sql.*;
import mondrian.spi.Dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Creates the right constraint for common tasks.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class SqlConstraintFactory {

    static boolean enabled;

    private static final SqlConstraintFactory instance =
        new SqlConstraintFactory();

    /**
     * singleton
     */
    private SqlConstraintFactory() {
    }

    private boolean enabled(final Evaluator context) {
        if (context != null) {
            return enabled && context.nativeEnabled();
        }
        return enabled;
    }

    public static SqlConstraintFactory instance() {
        setNativeNonEmptyValue();
        return instance;
    }

    public static void setNativeNonEmptyValue() {
        enabled = MondrianProperties.instance().EnableNativeNonEmpty.get();
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(
        Evaluator context)
    {
        if (!enabled(context)
            || !SqlContextConstraint.isValidContext(context, false))
        {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new SqlContextConstraint((RolapEvaluator) context, false);
    }

    public TupleConstraint getLevelMembersConstraint(Evaluator context) {
        return getLevelMembersConstraint(context, null);
    }

    /**
     * Returns a constraint that restricts the members of a level to those that
     * are non-empty in the given context. If the constraint cannot be
     * implemented (say if native constraints are disabled) returns null.
     *
     * @param context Context within which members must be non-empty
     * @param levels  levels being referenced in the current context
     * @return Constraint
     */
    public TupleConstraint getLevelMembersConstraint(
        Evaluator context,
        Level[] levels)
    {
        if (context == null) {
            return DefaultTupleConstraint.instance();
        }
        if (!enabled(context)) {
            return DefaultTupleConstraint.instance();
        }
        if (!SqlContextConstraint.isValidContext(
                context, false, levels, false))
        {
            return DefaultTupleConstraint.instance();
        }
        if (context.isNonEmpty()) {
            Set<CrossJoinArg> joinArgs =
                new CrossJoinArgFactory(false).buildConstraintFromAllAxes(
                    (RolapEvaluator) context);
            if (joinArgs.size() > 0) {
                return new RolapNativeCrossJoin.NonEmptyCrossJoinConstraint(
                    joinArgs.toArray(
                        new CrossJoinArg[joinArgs.size()]),
                    (RolapEvaluator) context);
            }
        }
        return new SqlContextConstraint((RolapEvaluator) context, false);
    }

    public MemberChildrenConstraint getChildByNameConstraint(
        RolapMember parent,
        Id.NameSegment childName)
    {
        // Ragged hierarchies span multiple levels, so SQL WHERE does not work
        // there
        if (!enabled || parent.getHierarchy().isRagged()) {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new ChildByNameConstraint(childName);
    }

    public MemberChildrenConstraint getChildByKeyConstraint(
        RolapMember parent,
        Id.KeySegment childKey)
    {
        // Ragged hierarchies span multiple levels, so SQL WHERE does not work
        // there
        if (!enabled || parent.getHierarchy().isRagged()) {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new ChildByKeyConstraint(childKey);
    }

    /**
     * Returns a constraint that allows to read all children of multiple parents
     * at once using a LevelMember query style. This does not work
     * for parent/child hierarchies.
     *
     * @param parentMembers List of parents (all must belong to same level)
     * @param mcc           The constraint that would return the children for
     *                      each single parent
     * @return constraint
     */
    public TupleConstraint getDescendantsConstraint(
        List<RolapMember> parentMembers,
        MemberChildrenConstraint mcc)
    {
        return new DescendantsConstraint(parentMembers, mcc);
    }

    /**
     * Gets a constraint for a key lookup on a level.
     * @param level Level being fetched
     * @param keyValues Member Key
     * @return Constraint
     */
    public TupleConstraint getMemberKeyConstraint(
        RolapLevel level, List<Comparable> keyValues)
    {
        List<Dialect.Datatype> datatypeList = new ArrayList<Dialect.Datatype>();
        List<MondrianDef.Expression> columnList =
            new ArrayList<MondrianDef.Expression>();
        for (RolapLevel x = level;; x = (RolapLevel) x.getParentLevel()) {
            if (x.getKeyExp() != null) {
                columnList.add(x.getKeyExp());
                datatypeList.add(x.getDatatype());
            }
            if (x.isUnique()) {
                break;
            }
        }
        return new MemberKeyConstraint(
            columnList,
            datatypeList,
            keyValues);
    }
}

// End SqlConstraintFactory.java
