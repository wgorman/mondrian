/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.Role.RollupPolicy;
import mondrian.rolap.RolapAggregator;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapCubeHierarchy;
import mondrian.rolap.RolapCubeLevel;
import mondrian.rolap.RolapCubeMember;
import mondrian.rolap.RolapEvaluator;

import org.eigenbase.util.property.IntegerProperty;

import java.util.*;

/**
 * Definition of the <code>AGGREGATE</code> MDX function.
 *
 * @author jhyde
 * @since 2005/8/14
 */
public class AggregateFunDef extends AbstractAggregateFunDef {

    private static final String TIMING_NAME =
        AggregateFunDef.class.getSimpleName();

    static final ReflectiveMultiResolver resolver =
        new ReflectiveMultiResolver(
            "Aggregate", "Aggregate(<Set>[, <Numeric Expression>])",
            "Returns a calculated value using the appropriate aggregate function, based on the context of the query.",
            new String[]{"fnx", "fnxn"},
            AggregateFunDef.class);

    /**
     * Creates an AggregateFunDef.
     *
     * @param dummyFunDef Dummy function
     */
    public AggregateFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final Calc calc =
            call.getArgCount() > 1
            ? compiler.compileScalar(call.getArg(1), true)
            : new ValueCalc(call);
        return new AggregateCalc(call, listCalc, calc);
    }

    public static class AggregateCalc extends GenericCalc {
        private final ListCalc listCalc;
        private final Calc calc;

        public AggregateCalc(Exp exp, ListCalc listCalc, Calc calc) {
            super(exp, new Calc[]{listCalc, calc});
            this.listCalc = listCalc;
            this.calc = calc;
        }

        public Object evaluate(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            try {
                TupleList list = evaluateCurrentList(listCalc, evaluator);
                return aggregate(calc, evaluator, list);
            } finally {
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }

        /**
         * This function converts many to many members to their corresponding
         * bridge members in many to many dimensions, allowing for the
         * aggregation to calculate correct results in the context of those
         * bridge members vs. the many to many members.
         *
         * There are optimizations that still could be implemented:
         *  - check to see if a single many to many member is in play, if so, 
         *    no need to replace it with it's bridge
         *  - we could execute a single custom query to determine the related
         *    bridge members for all many to many members of a specific
         *    hierarchy. At the moment we issue separate queries for each
         *    member in play.
         *  - we could improve the performance of deduplication of tuples
         *
         * @param evaluator the current evaluator
         * @param tupleList the original tuple list
         * @return a new tuple list
         */
        private static TupleList processManyToManyMembers(
            Evaluator evaluator, 
            TupleList tupleList) 
        {
            // first determine if many to many members are present in the tuple
            // list, we evaluate the first tuple.
            boolean m2m = false;
            List<Integer> m2mIndexes = new ArrayList<Integer>();
            List<RolapCubeHierarchy> m2mHierarchies =
                new ArrayList<RolapCubeHierarchy>();
            if (tupleList.size() == 0) {
              return tupleList;
            }
            
            List<Member> firstTuple = tupleList.get(0);

            RolapCube virtualCube = (RolapCube) evaluator.getCube();
            RolapCube baseCube = (RolapCube) evaluator.getMeasureCube();
          
            // for each member of the tuple, determine if it's in a many to
            // many hierarchy and if so add it to the index for later
            // processing 
            for (int i = 0; i < firstTuple.size(); i++) {
                Member m = firstTuple.get(i);
                RolapCubeHierarchy hier = null;
                if (!virtualCube.isVirtual()) {
                    if (m.getHierarchy() instanceof RolapCubeHierarchy) {
                        hier = (RolapCubeHierarchy)m.getHierarchy();
                    }
                } else {
                    // for virtual cubes, we need access to the base cube level
                    // this is dependent on the current measure at play.
                    RolapCubeLevel baseCubeLevel =
                        baseCube.findBaseCubeLevel(
                            (RolapCubeLevel)m.getLevel());
                    if (baseCubeLevel != null) {
                        hier = baseCubeLevel.getHierarchy();
                    }
                }
                if (hier != null && hier.getManyToManyHierarchy() != null) {
                    m2mIndexes.add(i);
                    m2mHierarchies.add(hier.getManyToManyHierarchy());
                    m2m = true;
                }
            }

            // note that another check that we can perform is if this m2m is
            // using only 1 member from each m2m dim, if so, no need to use
            // alternative join paths
            if (!m2m) {
                return tupleList;
            }

            TupleList newList = new ListTupleList(
                    tupleList.getArity() + m2mIndexes.size(),
                    new ArrayList<Member>()
                );
            final TupleCursor cursor = tupleList.tupleCursor();
            Member mlist[] = new Member[cursor.getArity() + m2mIndexes.size()];

            // for each tuple, lookup the list of members for each m2m member,
            // and then perform a cartesian product on those m2m values
            // resulting in new tuples
            while (cursor.forward()) {
                // future improvement - we could gather all the members from a
                // m2m dim and then issue a single SQL query to get their 
                // related m2m hierarchy members, then traverse the list again
                // populating the m2m hierarchy
                List<Set<Member>> newMembersList =
                    new ArrayList<Set<Member>>();
                for (int i = 0; i < cursor.getArity(); i++) {
                    if (m2mIndexes.contains(i)) {
                        int m2mIndex = m2mIndexes.indexOf(i);
                        Set<Member> newMembers = new TreeSet<Member>();
                        newMembersList.add(newMembers);
                        RolapCubeMember m = (RolapCubeMember)cursor.member(i);
                        RolapCubeHierarchy h = m2mHierarchies.get(m2mIndex);
                        Evaluator neweval = evaluator.push();
                        // clear out the slicer
                        List<Member> slicer = ((RolapEvaluator)neweval)
                            .getSlicerMembers();
                        for (Member sm : slicer) {
                            Member dm = sm.getHierarchy().getDefaultMember();
                            neweval.setContext(dm);
                        }
                        slicer.clear();
                        neweval.setContext(m);
                        // force native non-empty to on for this calculation.
                        // an alternative approach would be to throw an 
                        // exception if native non empty is not enabled.
                        boolean nativeNonEmpty = 
                            MondrianProperties.instance().EnableNativeNonEmpty
                                .get();
                        if (nativeNonEmpty == false) {
                            MondrianProperties.instance().EnableNativeNonEmpty
                                .set(true);
                        }
                        // we use the last level in the "bridge" hierarchy
                        List<Member> members = h.getRolapSchema()
                            .getSchemaReader().getLevelMembers(
                                h.getLevels()[h.getLevels().length - 1], 
                                neweval);
                        if (nativeNonEmpty == false) {
                            MondrianProperties.instance().EnableNativeNonEmpty
                                .set(false);
                        }
                        newMembers.addAll(members);

                        // change the scope of the m2m dim to be the all member
                        mlist[i] = m.getHierarchy().getAllMember();
                    } else {
                      // non m2m member
                      mlist[i] = cursor.member(i);
                    }
                }
            
                // build cartesian product of new members list
                List<List<Member>> totalmembers =
                    new ArrayList<List<Member>>();
                totalmembers.add(new ArrayList<Member>());
                for (int i = 0 ; i < newMembersList.size(); i++) {
                    Set<Member> newMembers = newMembersList.get(i);
                    if (i == newMembersList.size() - 1) {
                        // last iteration, add tuples
                        for (Member m : newMembers) {
                            for (List<Member> members : totalmembers) {
                                for (int j = 0; j < members.size(); j++) {
                                    mlist[mlist.length - (j+2)] =
                                        members.get(j);
                                }
                                // populate member array
                                mlist[mlist.length - 1] = m;
                                // future improvement - optimize dedupe
                                boolean duplicate = false;
                                for (int j = 0; j < newList.size(); j++) {
                                    List<Member> tc = newList.get(j);
                                    boolean matches = true;
                                    for (int k = 0;
                                        k < mlist.length && matches; k++) 
                                    {
                                        if (!mlist[k].equals(tc.get(k))) {
                                            matches = false;
                                        }
                                    }
                                    if (matches) {
                                        duplicate = true;
                                        break;
                                    }
                                }
                                if (!duplicate) {
                                    newList.addTuple(mlist);
                                }
                            }
                        }
                    } else {
                        // expand on total members with next level deep
                        List<List<Member>> ntotalmembers =
                            new ArrayList<List<Member>>();
                        for (Member m : newMembers) {
                            for (List<Member> members : totalmembers) {
                                List<Member> newmembers =
                                    new ArrayList<Member>(members);
                                newmembers.add(m);
                                ntotalmembers.add(newmembers);
                            }
                        }
                        totalmembers = ntotalmembers;
                    }
                }
            }
            return newList;
        }

        /**
         * Computes an expression for each element of a list, and aggregates
         * the result according to the evaluation context's current aggregation
         * strategy.
         *
         * @param calc Compiled expression to evaluate a scalar
         * @param evaluator Evaluation context
         * @param tupleList List of members or tuples
         * @return Aggregated result
         */
        public static Object aggregate(
            Calc calc,
            Evaluator evaluator,
            TupleList tupleList)
        {
            Aggregator aggregator =
                (Aggregator) evaluator.getProperty(
                    Property.AGGREGATION_TYPE.name, null);
            if (aggregator == null) {
                throw newEvalException(
                    null,
                    "Could not find an aggregator in the current evaluation context");
            }
            Aggregator rollup = aggregator.getRollup();
            if (rollup == null) {
                throw newEvalException(
                    null,
                    "Don't know how to rollup aggregator '" + aggregator + "'");
            }
            if (aggregator != RolapAggregator.DistinctCount) {
                tupleList = processManyToManyMembers(evaluator, tupleList);
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    final Object o =
                        rollup.aggregate(
                            evaluator, tupleList, calc);
                    return o;
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            // All that follows is logic for distinct count. It's not like the
            // other aggregators.
            if (tupleList.size() == 0) {
                return DoubleNull;
            }

            // Optimize the list
            // E.g.
            // List consists of:
            //  (Gender.[All Gender], [Product].[All Products]),
            //  (Gender.[F], [Product].[Drink]),
            //  (Gender.[M], [Product].[Food])
            // Can be optimized to:
            //  (Gender.[All Gender], [Product].[All Products])
            //
            // Similar optimization can also be done for list of members.

            if (evaluator instanceof RolapEvaluator
                && ((RolapEvaluator) evaluator).getDialect()
                .supportsUnlimitedValueList())
            {
                // If the DBMS does not have an upper limit on IN list
                // predicate size, then don't attempt any list
                // optimization, since the current algorithm is
                // very slow.  May want to revisit this if someone
                // improves the algorithm.
            } else {
                tupleList = optimizeTupleList(evaluator, tupleList, true);
            }

            // Can't aggregate distinct-count values in the same way
            // which is used for other types of aggregations. To evaluate a
            // distinct-count across multiple members, we need to gather
            // the members together, then evaluate the collection of
            // members all at once. To do this, we postpone evaluation,
            // and create a lambda function containing the members.
            Evaluator evaluator2 =
                evaluator.pushAggregation(tupleList);
            // cancel nonEmpty context
            evaluator2.setNonEmpty(false);
            return evaluator2.evaluateCurrent();
        }

        /**
         * Analyzes a list of tuples and determines if the list can
         * be safely optimized. If a member of the tuple list is on
         * a hierarchy for which a rollup policy of PARTIAL is set,
         * it is not safe to optimize that list.
         */
        private static boolean canOptimize(
            Evaluator evaluator,
            TupleList tupleList)
        {
            // If members of this hierarchy are controlled by a role which
            // enforces a rollup policy of partial, we cannot safely
            // optimize the tuples list as it might end up rolling up to
            // the parent while not all children are actually accessible.
            for (List<Member> tupleMembers : tupleList) {
                for (Member member : tupleMembers) {
                    final RollupPolicy policy =
                        evaluator.getSchemaReader().getRole()
                            .getAccessDetails(member.getHierarchy())
                                .getRollupPolicy();
                    if (policy == RollupPolicy.PARTIAL) {
                        return false;
                    }
                }
            }
            return true;
        }

        public static TupleList optimizeTupleList(
            Evaluator evaluator, TupleList tupleList, boolean checkSize)
        {
            if (!canOptimize(evaluator, tupleList)) {
                return tupleList;
            }

            // FIXME: We remove overlapping tuple entries only to pass
            // AggregationOnDistinctCountMeasuresTest
            // .testOptimizeListWithTuplesOfLength3 on Access. Without
            // the optimization, we generate a statement 7000
            // characters long and Access gives "Query is too complex".
            // The optimization is expensive, so we only want to do it
            // if the DBMS can't execute the query otherwise.
            if (false) {
                tupleList = removeOverlappingTupleEntries(tupleList);
            }
            tupleList =
                optimizeChildren(
                    tupleList,
                    evaluator.getSchemaReader(),
                    evaluator.getMeasureCube());
            if (checkSize) {
                checkIfAggregationSizeIsTooLarge(tupleList);
            }
            return tupleList;
        }

        /**
         * In case of distinct count aggregation if a tuple which is a super
         * set of other tuples in the set exists then the child tuples can be
         * ignored.
         *
         * <p>For example. A list consisting of:
         *  (Gender.[All Gender], [Product].[All Products]),
         *  (Gender.[F], [Product].[Drink]),
         *  (Gender.[M], [Product].[Food])
         * Can be optimized to:
         *  (Gender.[All Gender], [Product].[All Products])
         *
         * @param list List of tuples
         */
        public static TupleList removeOverlappingTupleEntries(
            TupleList list)
        {
            TupleList trimmedList = list.cloneList(list.size());
            Member[] tuple1 = new Member[list.getArity()];
            Member[] tuple2 = new Member[list.getArity()];
            final TupleCursor cursor1 = list.tupleCursor();
            while (cursor1.forward()) {
                cursor1.currentToArray(tuple1, 0);
                if (trimmedList.isEmpty()) {
                    trimmedList.addTuple(tuple1);
                } else {
                    boolean ignore = false;
                    final TupleIterator iterator = trimmedList.tupleIterator();
                    while (iterator.forward()) {
                        iterator.currentToArray(tuple2, 0);
                        if (isSuperSet(tuple1, tuple2)) {
                            iterator.remove();
                        } else if (isSuperSet(tuple2, tuple1)
                            || isEqual(tuple1, tuple2))
                        {
                            ignore = true;
                            break;
                        }
                    }
                    if (!ignore) {
                        trimmedList.addTuple(tuple1);
                    }
                }
            }
            return trimmedList;
        }

        /**
         * Returns whether tuple1 is a superset of tuple2.
         *
         * @param tuple1 First tuple
         * @param tuple2 Second tuple
         * @return boolean Whether tuple1 is a superset of tuple2
         */
        public static boolean isSuperSet(Member[] tuple1, Member[] tuple2) {
            int parentLevelCount = 0;
            for (int i = 0; i < tuple1.length; i++) {
                Member member1 = tuple1[i];
                Member member2 = tuple2[i];

                if (!member2.isChildOrEqualTo(member1)) {
                    return false;
                }
                if (member1.getLevel().getDepth()
                    < member2.getLevel().getDepth())
                {
                    parentLevelCount++;
                }
            }
            return parentLevelCount > 0;
        }

        private static void checkIfAggregationSizeIsTooLarge(List list) {
            final IntegerProperty property =
                MondrianProperties.instance().MaxConstraints;
            final int maxConstraints = property.get();
            if (list.size() > maxConstraints) {
                throw newEvalException(
                    null,
                    "Aggregation is not supported over a list"
                    + " with more than " + maxConstraints + " predicates"
                    + " (see property " + property.getPath() + ")");
            }
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            if (hierarchy.getDimension().isMeasures()) {
                return true;
            }
            return anyDependsButFirst(getCalcs(), hierarchy);
        }

        /**
         * In distinct Count aggregation, if tuple list is a result
         * m.children * n.children then it can be optimized to m * n
         *
         * <p>
         * E.g.
         * List consist of:
         *  (Gender.[F], [Store].[USA]),
         *  (Gender.[F], [Store].[USA].[OR]),
         *  (Gender.[F], [Store].[USA].[CA]),
         *  (Gender.[F], [Store].[USA].[WA]),
         *  (Gender.[F], [Store].[CANADA])
         *  (Gender.[M], [Store].[USA]),
         *  (Gender.[M], [Store].[USA].[OR]),
         *  (Gender.[M], [Store].[USA].[CA]),
         *  (Gender.[M], [Store].[USA].[WA]),
         *  (Gender.[M], [Store].[CANADA])
         * Can be optimized to:
         *  (Gender.[All Gender], [Store].[USA])
         *  (Gender.[All Gender], [Store].[CANADA])
         *
         *
         * @param tuples Tuples
         * @param reader Schema reader
         * @param baseCubeForMeasure Cube
         * @return xxxx
         */
        public static TupleList optimizeChildren(
            TupleList tuples,
            SchemaReader reader,
            Cube baseCubeForMeasure)
        {
            Map<Member, Integer>[] membersOccurencesInTuple =
                membersVersusOccurencesInTuple(tuples);
            int tupleLength = tuples.getArity();

            //noinspection unchecked
            Set<Member>[] sets = new Set[tupleLength];
            boolean optimized = false;
            for (int i = 0; i < tupleLength; i++) {
                if (areOccurencesEqual(membersOccurencesInTuple[i].values())) {
                    Set<Member> members = membersOccurencesInTuple[i].keySet();
                    int originalSize = members.size();
                    sets[i] =
                        optimizeMemberSet(
                            new LinkedHashSet<Member>(members),
                            reader,
                            baseCubeForMeasure);
                    if (sets[i].size() != originalSize) {
                        optimized = true;
                    }
                } else {
                    sets[i] =
                        new LinkedHashSet<Member>(
                            membersOccurencesInTuple[i].keySet());
                }
            }
            if (optimized) {
                return crossProd(sets);
            }
            return tuples;
        }

        /**
         * Finds member occurrences in tuple and generates a map of Members
         * versus their occurrences in tuples.
         *
         * @param tupleList List of tuples
         * @return Map of the number of occurrences of each member in a tuple
         */
        public static Map<Member, Integer>[] membersVersusOccurencesInTuple(
            TupleList tupleList)
        {
            int tupleLength = tupleList.getArity();
            //noinspection unchecked
            Map<Member, Integer>[] counters = new Map[tupleLength];
            for (int i = 0; i < counters.length; i++) {
                counters[i] = new LinkedHashMap<Member, Integer>();
            }
            for (List<Member> tuple : tupleList) {
                for (int i = 0; i < tuple.size(); i++) {
                    Member member = tuple.get(i);
                    Map<Member, Integer> map = counters[i];
                    if (map.containsKey(member)) {
                        Integer count = map.get(member);
                        map.put(member, ++count);
                    } else {
                        map.put(member, 1);
                    }
                }
            }
            return counters;
        }

        private static Set<Member> optimizeMemberSet(
            Set<Member> members,
            SchemaReader reader,
            Cube baseCubeForMeasure)
        {
            boolean didOptimize;
            Set<Member> membersToBeOptimized = new LinkedHashSet<Member>();
            Set<Member> optimizedMembers = new LinkedHashSet<Member>();
            while (members.size() > 0) {
                Iterator<Member> iterator = members.iterator();
                Member first = iterator.next();
                if (first.isAll()) {
                    optimizedMembers.clear();
                    optimizedMembers.add(first);
                    return optimizedMembers;
                }
                membersToBeOptimized.add(first);
                iterator.remove();

                Member firstParentMember = first.getParentMember();
                while (iterator.hasNext()) {
                    Member current =  iterator.next();
                    if (current.isAll()) {
                        optimizedMembers.clear();
                        optimizedMembers.add(current);
                        return optimizedMembers;
                    }

                    Member currentParentMember = current.getParentMember();
                    if (firstParentMember == null
                        && currentParentMember == null
                        || (firstParentMember != null
                        && firstParentMember.equals(currentParentMember)))
                    {
                        membersToBeOptimized.add(current);
                        iterator.remove();
                    }
                }

                int childCountOfParent = -1;
                if (firstParentMember != null) {
                    childCountOfParent =
                        getChildCount(firstParentMember, reader);
                }
                if (childCountOfParent != -1
                    && membersToBeOptimized.size() == childCountOfParent
                    && canOptimize(firstParentMember, baseCubeForMeasure))
                {
                    optimizedMembers.add(firstParentMember);
                    didOptimize = true;
                } else {
                    optimizedMembers.addAll(membersToBeOptimized);
                    didOptimize = false;
                }
                membersToBeOptimized.clear();

                if (members.size() == 0 && didOptimize) {
                    Set temp = members;
                    members = optimizedMembers;
                    optimizedMembers = temp;
                }
            }
            return optimizedMembers;
        }

        /**
         * Returns whether tuples are equal. They must have the same length.
         *
         * @param tuple1 First tuple
         * @param tuple2 Second tuple
         * @return whether tuples are equal
         */
        private static boolean isEqual(Member[] tuple1, Member[] tuple2) {
            for (int i = 0; i < tuple1.length; i++) {
                if (!tuple1[i].getUniqueName().equals(
                        tuple2[i].getUniqueName()))
                {
                    return false;
                }
            }
            return true;
        }

        private static boolean canOptimize(
            Member parentMember,
            Cube baseCube)
        {
            return dimensionJoinsToBaseCube(
                parentMember.getDimension(), baseCube)
                || !parentMember.isAll();
        }

        private static TupleList crossProd(Set<Member>[] sets) {
            final List<TupleList> tupleLists = new ArrayList<TupleList>();
            for (Set<Member> set : sets) {
                tupleLists.add(
                    new UnaryTupleList(
                        new ArrayList<Member>(set)));
            }
            if (tupleLists.size() == 1) {
                return tupleLists.get(0);
            }
            return CrossJoinFunDef.mutableCrossJoin(tupleLists);
        }

        private static boolean dimensionJoinsToBaseCube(
            Dimension dimension,
            Cube baseCube)
        {
            HashSet<Dimension> dimensions = new HashSet<Dimension>();
            dimensions.add(dimension);
            return baseCube.nonJoiningDimensions(dimensions).size() == 0;
        }

        private static int getChildCount(
            Member parentMember,
            SchemaReader reader)
        {
            int childrenCountFromCache =
                reader.getChildrenCountFromCache(parentMember);
            if (childrenCountFromCache != -1) {
                return childrenCountFromCache;
            }
            return reader.getMemberChildren(parentMember).size();
        }
    }
}

// End AggregateFunDef.java
