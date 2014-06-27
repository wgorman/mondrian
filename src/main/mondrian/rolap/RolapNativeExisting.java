/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2014 Pentaho
// All rights reserved.
*/
package mondrian.rolap;

import mondrian.calc.TupleList;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.SqlQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

public class RolapNativeExisting extends RolapNativeSet {

    public RolapNativeExisting() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeExisting.get());
    }

    protected boolean restrictMemberTypes() {
        return true;
    }

    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        if (!isEnabled()) {
            return null;
        }
        if (!ExistingConstraint.isValidContext(
            evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }
        // attempt to nativize the set expression
        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

        if (failedCjArg(allArgs)) {
            alertNonNative(evaluator, fun, args[0]);
            return null;
        }
        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }
        final CrossJoinArg[] contextPredicateArgs =
            getContextArgsByDim(cjArgs, evaluator, restrictMemberTypes());
        if (contextPredicateArgs == null) {
            alertNonNative(evaluator, fun, args[0]);
        }
        final CrossJoinArg[] predicateArgs = allArgs.size() > 1
            ? Util.appendArrays(contextPredicateArgs, allArgs.get(1))
            : contextPredicateArgs;
        final CrossJoinArg[] combinedArgs =
              Util.appendArrays(cjArgs, predicateArgs);

        ExistingConstraint constraint = new ExistingConstraint(
            combinedArgs,
            evaluator);
        return createNativeEvaluator(evaluator, cjArgs, constraint);
    }

    public SetEvaluator createNativeEvaluator(
        RolapEvaluator evaluator,
        CrossJoinArg[] cjArgs,
        SetConstraint constraint)
    {
        LOGGER.debug("native EXISTING");
        //RolapEvaluator setEvaluator = evaluator.push();
        // overrideContext(setEvaluator, cjArgs, null);
        return new SetEvaluator(
            cjArgs,
            evaluator.getSchemaReader(),
            constraint);
    }

    /**
     * Get all context members from the same dimensions as the provided cjArgs
     */
    public static CrossJoinArg[] getContextArgsByDim(
      CrossJoinArg[] cjArgs,
      RolapEvaluator evaluator,
      boolean restrictMemberTypes)
    {
        ArrayList<CrossJoinArg> contextPredicates =
            new ArrayList<CrossJoinArg>();
        HashSet<Dimension> processedDims = new HashSet<Dimension>();
        for (CrossJoinArg cjArg : cjArgs) {
            // get each relevant context member
            RolapLevel level = cjArg.getLevel();
            if (level != null) {
                Dimension dimension = level.getHierarchy().getDimension();
                if (!processedDims.add(dimension)) {
                    // avoid repeated contextMembers
                    continue;
                }
                for (RolapMember contextMember
                    : getContext(evaluator, dimension))
                {
                    if (!contextMember.isAll()) {
                        CrossJoinArg predicate =
                            MemberListCrossJoinArg.create(
                                evaluator,
                                Collections.singletonList(contextMember),
                                restrictMemberTypes, false);
                        if (predicate == null){ 
                            return null;
                        }
                        contextPredicates.add(predicate);
                    }
                }
            }
        }
        return contextPredicates.toArray(
            new CrossJoinArg[contextPredicates.size()]);
    }

    private static List<RolapMember> getContext(
        RolapEvaluator evaluator,
        Dimension dim)
    {
        Hierarchy[] dimHierarchies = dim.getHierarchies();
        List<RolapMember> contextMembers =
            new ArrayList<RolapMember>(dimHierarchies.length);
        for(Hierarchy hierarchy : dimHierarchies) {
            contextMembers.add(evaluator.getContext(hierarchy));
        }
        return contextMembers;
    }

    public static class ExistingConstraint extends SetConstraint {
        // only need the context
        public ExistingConstraint(CrossJoinArg[] args, RolapEvaluator evaluator) {
            super(args, evaluator, true);
        }
        protected boolean isJoinRequired() {
            return false;
        }
        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            //super.addConstraint(sqlQuery, baseCube, aggStar);
            for (CrossJoinArg arg : args) {
                // if the cross join argument has calculated members in its
                // enumerated set, ignore the constraint since we won't
                // produce that set through the native sql and instead
                // will simply enumerate through the members in the set
                if (!(arg instanceof MemberListCrossJoinArg)
                    || !((MemberListCrossJoinArg) arg).hasCalcMembers())
                {
                    RolapLevel level = arg.getLevel();
                    if (level == null || levelIsOnBaseCube(baseCube, level)) {
                        arg.addConstraint(sqlQuery, baseCube, aggStar);
                    }
                }
            }
        }
    }

    /**
     * Removes tuples containing a member in the given set
     */
    private static TupleList removeInvalidTuples(
        TupleList tuples,
        Set<RolapMember> removedMembers)
    {
        ListIterator<List<Member>> tupleIter = tuples.listIterator();
        iterateLeft:
        while (tupleIter.hasNext()) {
            // remove tuples containing removed members
            for (Member member : tupleIter.next()) {
                if (removedMembers.contains(member)) {
                    tupleIter.remove();
                    continue iterateLeft;
                }
            }
        }
        return tuples;
    }

    /**
     * Refetches members from left set too see which are eliminated by
     * 
     * @param evaluator
     * @param hierarchyLeft
     * @param leftSet
     * @param hierarchyRight
     * @param rightSet
     * @return
     */
    public static TupleList postFilterExistingRelatedHierarchies(
      RolapEvaluator evaluator,
      RolapHierarchy hierarchyLeft, TupleList leftSet,
      Hierarchy hierarchyRight, TupleList rightSet)
    {
        Map<RolapLevel,List<RolapMember>> leftMembers =
            extractHierarchyMembers(hierarchyLeft, leftSet);
        Map<RolapLevel,List<RolapMember>> rightMembers =
            extractHierarchyMembers(hierarchyRight, rightSet);
        Iterable<List<RolapMember>> leftChunks =
            getMemberLists(leftMembers);
        Iterable<List<RolapMember>> rightChunks =
            getMemberLists(rightMembers);
        // determine which members will be removed by any of the constraints
        Set<RolapMember> removedMembers = new HashSet<RolapMember>();
        for (List<RolapMember> left : leftChunks) {
            for (List<RolapMember> right : rightChunks) {
                Set<RolapMember> removedMembersPart =
                  new HashSet<RolapMember>(left);
                SqlTupleReader reader =
                    getFilterExistingReader(evaluator, left, right);
                // unary tuple list
                TupleList members = reader.readMembers(
                  evaluator.getSchemaReader().getDataSource(), null, null);
                List<Member> filteredLeftMembers = members.slice(0);
                removedMembersPart.removeAll(filteredLeftMembers);
                removedMembers.addAll(removedMembersPart);
            }
        }
        return removeInvalidTuples(leftSet, removedMembers);
    }

    private static Iterable<List<RolapMember>> getMemberLists(
        Map<RolapLevel, List<RolapMember>> memberMap)
    {
        // separate in chunks according to MaxConstraints
        List<List<RolapMember>> result = new ArrayList<List<RolapMember>>();
        final int maxSize = MondrianProperties.instance().MaxConstraints.get();
        for (List<RolapMember> members : memberMap.values()) {
            if (members.size() <= maxSize) {
                result.add(members);
            }
            else {
                //split
                while (members != null) {
                    boolean last = members.size() < maxSize;
                    List<RolapMember> membersSub =
                        members.subList(0, last ? members.size() : maxSize);
                    result.add(membersSub);
                    members = last
                        ? null
                        : members.subList(maxSize, members.size());
                }
            }
        }
        return result;
    }

    private static SqlTupleReader getFilterExistingReader(
        RolapEvaluator evaluator,
        List<RolapMember> toFetch,
        List<RolapMember> toConstrain)
    {
        CrossJoinArg[] args = new CrossJoinArg[2];
        args[0] = MemberListCrossJoinArg.create(
            evaluator, toFetch, false, false);
        args[1] = MemberListCrossJoinArg.create(
          evaluator, toConstrain, false, false);
        // members and constraint to where
        ExistingConstraint constraint = new ExistingConstraint(args, evaluator);
        SqlTupleReader reader = new SqlTupleReader(constraint);
        // add level to select
        reader.addLevelMembers(
            args[0].getLevel(),
            args[0].getLevel().getHierarchy()
                .getMemberReader().getMemberBuilder(),
            null);
        return reader;
    }

    private static Map<RolapLevel,List<RolapMember>> extractHierarchyMembers(
      Hierarchy targetHierarchy,
      TupleList tuples)
    {
        Map<RolapLevel, List<RolapMember>> result =
            new HashMap<RolapLevel, List<RolapMember>>();
        for(List<Member> tuple : tuples) {
            for (Member member : tuple) {
                if (member.getHierarchy().equals(targetHierarchy)
                    && member instanceof RolapMember)
                {
                    // avoiding rolapCubeMember will prevent fact table joins
                    // TODO: a better way
                    RolapMember rolapMember = (RolapMember) member;
                    if (rolapMember instanceof RolapCubeMember) {
                        rolapMember = ((RolapCubeMember)member).getRolapMember();
                    }
                    putInMapList(
                        result,
                        rolapMember.getLevel(),
                        rolapMember);
                    continue;
                }
            }
        }
        return result;
    }
    private static <K,V> void putInMapList(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<V>();
            map.put(key, list);
        }
        list.add(value);
    }

}
// End RolapNativeExisting.java