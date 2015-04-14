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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.util.Pair;

/**
 * Porvides utilities for determining if two constraints contain members that can be used to
 * consolidate multiple SQL queries into a single query using an IN clause.
 */
public class ConsolidationUtils {

    private ConsolidationUtils() {
    }

    /**
     * Logger that groups consolidation logging messages.
     */
    public static Logger CONSOLIDATION_LOGGER = Logger.getLogger("mondrian.rolap.CONSOLIDATION");

    /**
     * Get the list of members and cross join args that will be used to determine if two
     * constraints
     * overlap to correct degree, i.e., are the same apart from a single member each in the same
     * level.
     *
     * @param key1
     * @param key2
     * @return ContextualObjects or null if the constraints do not overlap correctly.
     */
    protected static ConstraintObject getConstraintObject(CacheKey key1,
                                                          CacheKey key2) {
        if(CONSOLIDATION_LOGGER.isTraceEnabled()) {
            CONSOLIDATION_LOGGER.trace("Cache Keys are: " + key1 + ":" + key2);
        }
        if (key1.size() != key2.size()) {
            return null;
        }

        // we want everything to match except members and crossjoin args.
        CacheKey other1 = key1
            .getCacheKeyWithoutKeys(CacheKey.KEY_MEMBERS, CacheKey.KEY_CROSSJOIN_ARGS,
                RolapNativeSet.SetEvaluator.KEY_SET_EVALUATOR_CROSSJOIN_ARGS);
        CacheKey other2 = key2
            .getCacheKeyWithoutKeys(CacheKey.KEY_MEMBERS, CacheKey.KEY_CROSSJOIN_ARGS,
                RolapNativeSet.SetEvaluator.KEY_SET_EVALUATOR_CROSSJOIN_ARGS);
        if(!other1.equals(other2)) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("parts of the keys do not match.");
            }
            return null;
        }

        // evaluator members
        List<Member> members1 = key1.getMembers();
        List<Member> members2 = key2.getMembers();

        if (members1.size() != members2.size()) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("member lists are not the same size");
            }
            return null;
        }

        //List<CrossJoinArg> cross join args
        List<CrossJoinArg> args1 = key1.getCrossJoinArgs();
        List<CrossJoinArg> args2 = key2.getCrossJoinArgs();

        if (args1 != null && args2 != null && args1.size() != args2.size()) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("cross join args do not share nullity");
            }
            return null;
        }
        if (args1 == null && args2 != null) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("cross join args do not share nullity");
            }
            return null;
        }
        if (args2 == null && args1 != null) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("cross join args do not share nullity");
            }
            return null;
        }
        return new ConstraintObject(members1, args1, members2, args2);
    }

    /**
     * Returns a level match given two constraints.
     * Currently only a single level match is supported, and there can only be one.
     * If more than one could be returned, then this returns null.
     *
     * @param key1
     * @param key2
     * @return a LevelMatch object or null if one cannot be determined.
     */
    public static LevelMatch getLevelMatch(CacheKey key1, CacheKey key2) {

        ConstraintObject objects = getConstraintObject(key1, key2);
        if (objects == null) {
            return null;
        }
        LevelMatch levelMatch = null;
        int index = 0;

        if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
            CONSOLIDATION_LOGGER.trace("Cross join args size:" + objects.crossJoinArgs1.size());
        }
        for (int i = 0; i < objects.crossJoinArgs1.size(); i++) {
            CrossJoinArg crossJoinArg1 = objects.crossJoinArgs1.get(i);
            CrossJoinArg crossJoinArg2 = objects.crossJoinArgs2.get(i);
            if (crossJoinArg1 instanceof MemberListCrossJoinArg
                && crossJoinArg2 instanceof MemberListCrossJoinArg) {
                if (!((MemberListCrossJoinArg) crossJoinArg1).hasCalcMembers()
                    && !((MemberListCrossJoinArg) crossJoinArg2).hasCalcMembers()) {
                    List<RolapMember> m1 = crossJoinArg1.getMembers();
                    List<RolapMember> m2 = crossJoinArg2.getMembers();
                    if (m1.size() != m2.size()) {
                        if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                            CONSOLIDATION_LOGGER.trace(
                                "Member Cross join args member list sizes do not match:"
                                    + m1
                                    + " : "
                                    + m2);
                        }
                        return null;
                    }
                    for (int j = 0; j < m1.size(); j++) {
                        RolapMember member1 = m1.get(j);
                        RolapMember member2 = m2.get(j);
                        Pair<Integer, LevelMatch> pair = evaluateMatch(member1, member2, levelMatch,
                            index, "Member Cross Join", true, false);
                        LevelMatch currMatch = pair.right;
                        int val = pair.left;
                        if (currMatch != null) {
                            levelMatch = currMatch;
                        } else {
                            if (val != 0) {
                                return null;
                            }
                        }
                        index++;
                    }
                }
            } else if (crossJoinArg1 instanceof DescendantsCrossJoinArg
                && crossJoinArg2 instanceof DescendantsCrossJoinArg) {
                List<RolapMember> m1 = crossJoinArg1.getMembers();
                List<RolapMember> m2 = crossJoinArg2.getMembers();
                if (m1 == null || m2 == null) {
                    if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                        CONSOLIDATION_LOGGER.trace(
                            "Descendant Cross join args members are null. Continuing.");
                    }
                    continue;
                }
                if (m1.size() != m2.size()) {  // must be one in this case
                    if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                        CONSOLIDATION_LOGGER.trace(
                            "Descendant Cross join args member list sizes do not match:"
                                + m1
                                + " : "
                                + m2);
                    }
                    return null;
                }
                for (int j = 0; j < m1.size(); j++) {
                    RolapMember member1 = m1.get(j);
                    RolapMember member2 = m2.get(j);
                    Pair<Integer, LevelMatch> pair = evaluateMatch(member1, member2, levelMatch,
                        index, "Descendant Cross Join", true, true);
                    LevelMatch currMatch = pair.right;
                    int val = pair.left;
                    if (currMatch != null) {
                        levelMatch = currMatch;
                    } else {
                        if (val != 0) {
                            return null;
                        }
                    }
                    index++;
                }
            } else {
                return null;
            }
        }

        if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
            CONSOLIDATION_LOGGER.trace("members size:" + objects.members1.size());
        }
        for (int i = 0; i < objects.members1.size(); i++) {

            Member member1 = objects.members1.get(i);
            Member member2 = objects.members2.get(i);

            Pair<Integer, LevelMatch> pair = evaluateMatch(member1, member2, levelMatch, index,
                "Member", false, false);
            LevelMatch currMatch = pair.right;
            int val = pair.left;
            if (currMatch != null) {
                levelMatch = currMatch;
            } else {
                if (val != 0) {
                    return null;
                }
            }
            index++;
        }
        return levelMatch;
    }

    /**
     * Evaluates a LevelMatch given two members and returns a Pair containing a status integer and
     * a
     * possibly null Level member. If the integer is 0, then the LevelMember is null and the two
     * members are equivalent. This usually means a later match can be found, or the existing
     * match,
     * if not null, is still valid. If the the integer is -1, then the LevelMember is null and
     * the members cannot be matched. If the integer is 2, then the LevelMember is null, because
     * the members match, but the existing
     * LevelMember is not null, therefore we would have two matches. If the integer is 1, then the
     * members match and there is no existing LevelMember, but the new LevelMember could not be
     * created, for example because the member values represent NULL. The latter should not
     * actually happen as member.isNull() is checked during the match.
     *
     * @param member1             the first member
     * @param member2             the second member
     * @param existing            an existing LevelMatch.
     * @param index               an index used to add to the level member to help matching multiple
     *                            members
     *                            down stream.
     * @param memberType          the type of member. This is just for logging purposes.
     * @param crossJoin           whether or not the members are part of a Member list cross join
     * @param descendantCrossJoin whether or not the members are part of a Descendant cross join
     * @return a Pair containing a status integer and a possibly null LevelMember.
     */
    private static Pair<Integer, LevelMatch> evaluateMatch(Member member1, Member member2,
                                                           LevelMatch existing, int index,
                                                           String memberType, boolean crossJoin,
                                                           boolean descendantCrossJoin) {
        int match = areLevelMembers(member1, member2);
        if (match == 0) {
            return Pair.of(0, null);
        } else if (match == -1) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace(
                    memberType + " match cannot be matched for:" + member1 + " : " + member2);
            }
            return Pair.of(-1, null);
        } else {
            if (existing != null) {
                if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                    CONSOLIDATION_LOGGER.trace(
                        memberType + " more than one match for:" + member1 + " : " + member2);
                }
                return Pair.of(2, null);
            }
            MondrianDef.Column column = getColumn((RolapCubeMember) member1);
            // safe cast - areLevelMembers will only match on cube members
            LevelMatch levelMatch = createMatch(column, (RolapCubeMember) member1,
                (RolapCubeMember) member2, index, crossJoin, descendantCrossJoin);
            if (levelMatch == null) {
                if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                    CONSOLIDATION_LOGGER.trace(memberType
                        + " level members could not be created for:"
                        + member1
                        + " : "
                        + member2);
                }
                return Pair.of(1, null);
            }
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Created match from " + memberType + ": " + levelMatch);
            }
            return Pair.of(1, levelMatch);
        }

    }

    /**
     * Get the column for a LevelMatch object. Not all columns are fully populated at this point,
     * it appears.
     *
     * @param member
     * @return
     */
    private static MondrianDef.Column getColumn(RolapCubeMember member) {
        RolapCubeLevel level = member.getLevel();
        MondrianDef.Expression expr = level.getKeyExp();
        if (expr instanceof MondrianDef.Column) {
            MondrianDef.Column col = (MondrianDef.Column) expr;
            if (col.getTableAlias() != null && col.getName() != null) {
                return col;
            }
        }
        String msg = "getColumn: have to create a column object for level " + level.getKeyExp();
        CONSOLIDATION_LOGGER.warn(msg);
        if(CONSOLIDATION_LOGGER.isDebugEnabled()) {
            Exception ex = new Exception(msg);
            CONSOLIDATION_LOGGER.debug(msg, ex);
        }
        return new MondrianDef.Column(level.getTableAlias(),
            level.getKeyExp().getGenericExpression());
    }

    /**
     * Returns a LevelMemberPair or null if one of the values is a null value.
     *
     * @param column
     * @param member1
     * @param member2
     * @param index
     * @return
     */
    private static LevelMatch createMatch(MondrianDef.Column column, RolapCubeMember member1,
                                          RolapCubeMember member2, int index, boolean crossJoin,
                                          boolean descendantCrossJoin) {
        Object key1 = member1.getKey();
        Object key2 = member2.getKey();
        if (key1 == RolapUtil.sqlNullValue || key2 == RolapUtil.sqlNullValue) {
            return null;
        }
        return new LevelMatch(column, index, member1, member2, crossJoin, descendantCrossJoin);
    }

    /**
     * Returns 0 = false, 1 = true, -1 = cannot match
     * False means they are equal, so this is not a match, but a later match is possible.
     *
     * @param m1
     * @param m2
     * @return
     */
    private static int areLevelMembers(Member m1, Member m2) {
        // if they are exactly the same - return null
        if (m1.equals(m2)) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Members are equal:" + m1 + " : " + m2);
            }
            return 0;
        }
        // should be the same class - if not - return null
        if (!m1.getClass().equals(m2.getClass())) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Classes do not match:" + m1 + " : " + m2);
            }
            return -1;
        }
        // we want rolap cube members only, at this point
        if (!(m1 instanceof RolapCubeMember) || !(m2 instanceof RolapCubeMember)) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Can only match cube members:" + m1.getClass()
                    + " : " + m2.getClass());
            }
            return -1;
        }
        RolapCubeMember mem1 = (RolapCubeMember) m1;
        RolapCubeMember mem2 = (RolapCubeMember) m2;
        if (mem1.isAll() || mem1.isNull() || mem1.isCalculated()) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Member is either all, null or calculated:" + mem1);
            }
            return -1;
        }
        if (mem2.isAll() || mem2.isNull() || mem2.isCalculated()) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Member is either all, null or calculated:" + mem2);
            }
            return -1;
        }
        if (!mem1.getDimension().equals(mem2.getDimension())) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Dimensions do not match:" + mem1 + " : " + mem2);
            }
            return -1;
        }
        if (!mem1.getLevel().equals(mem2.getLevel())) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("Levels do not match:" + mem1 + " : " + mem2);
            }
            return -1;
        }
        if(mem1.getParentMember() == null && mem2.getParentMember() != null) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("One member has a null parent:" + mem1 + " : " + mem2);
            }
            return -1;
        }
        if (mem2.getParentMember() == null && mem1.getParentMember() != null) {
            if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                CONSOLIDATION_LOGGER.trace("One member has a null parent:" + mem1 + " : " + mem2);
            }
            return -1;
        }
        if (mem1.getParentMember() != null && mem2.getParentMember() != null) {
            if (!mem1.getParentMember()
                     .getUniqueName()
                     .equals(mem2.getParentMember().getUniqueName())) {
                if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
                    CONSOLIDATION_LOGGER.trace("Parents do not match:"
                        + mem1.getParentMember()
                        + ":"
                        + mem2.getParentMember());
                }
                return -1;
            }
        }
        if (CONSOLIDATION_LOGGER.isTraceEnabled()) {
            CONSOLIDATION_LOGGER.trace("Found match:" + mem1 + " : " + mem2);
        }
        return 1;
    }

    /**
     * Groups a list of native requests into a pair of list of lists with each list
     * containing a Pair. The pair.left is the request, the pair.right
     * is a level match object. Every pair's left is the same, i.e.,
     * each list is built up by testing against the first member of an existing
     * list. The first request in a list is special as this has the constraint
     * that has been augmented with the level members, so is the request that
     * should be used to execute the consolidated query.
     * Requests that cannot be grouped are added as a list of pairs of size 1.
     * Requests are grouped taking into account the thresholds, i.e. each list of grouped
     * requests is no longer than multiThreshold and no list of single requests is longer than
     * singleThreshold.
     *
     * @param requests        a list of native requests.
     * @param <R>             objects that extend NativeRequest
     * @param singleThreshold maximum number of single requests that should be aggregated into a
     *                        single request.
     * @param multiThreshold  maximum number of grouped requests that should be aggregated into a
     *                        single request.
     * @return a pair of list of lists containing pairs, i.e.,
     * List<List<Pair<R, ConsolidationUtils.LevelMemberPair>>>
     * The Pair's.left contains the list of lists of aggregated single queries.
     * The Pair's.right contains the list of lists of grouped queries.
     */

    public static <R extends RolapNativeSet.NativeRequest> Pair<List<List<Pair<R,
        ConsolidationUtils.LevelMatch>>>, List<List<Pair<R,
        ConsolidationUtils.LevelMatch>>>> groupPairs(
        ConsolidationHandler.ConsolidationHandlerFactory factory, List<R> requests,
        int singleThreshold, int multiThreshold) {
        List<List<Pair<R, ConsolidationUtils.LevelMatch>>> orderedRequests
            = new ArrayList<List<Pair<R, LevelMatch>>>();

        for (R request : requests) {
            boolean added = false;
            TupleConstraint c = request.getSetEvaluator().getConstraint();
            if (c instanceof RolapNativeSet.SetConstraint) {

                for (List<Pair<R, ConsolidationUtils.LevelMatch>> orderedRequest
                    : orderedRequests) {
                    if (orderedRequest.size() >= multiThreshold) {
                        continue;
                    }
                    Pair<R, ConsolidationUtils.LevelMatch> first = orderedRequest.get(0);
                    R existingRequest = first.left;
                    RolapNativeSet.SetConstraint existing
                        = (RolapNativeSet.SetConstraint) existingRequest.getSetEvaluator()
                                                                        .getConstraint();
                    ConsolidationUtils.LevelMatch match = ConsolidationUtils.getLevelMatch(
                        existingRequest.getCacheKey(), request.getCacheKey());

                    if (match != null) {
                        // there are already matches. Need to make sure the pairs are the same.
                        boolean noMatch = false;
                        if (orderedRequest.size() > 1) {
                            for (int i = 1; i < orderedRequest.size(); i++) {
                                ConsolidationUtils.LevelMatch currMatch
                                    = orderedRequest.get(i).right;
                                if (!currMatch.matches(match)) {
                                    ConsolidationUtils.CONSOLIDATION_LOGGER.debug(
                                        "failed to match:" + match + " against " + currMatch);
                                    noMatch = true;
                                    break;
                                }
                            }
                        }
                        if (noMatch) {
                            continue;
                        }
                        ConsolidationHandler handler = existing.getConsolidationHandler();
                        if (handler == null) {
                            handler = factory.newConsolidationHandler();
                            existing.setConsolidationHandler(handler);
                        }
                        // we add all the members to the first constraint only
                        if (match.isCrossJoin() && match.isDescendantCrossJoin()) {
                            handler.addDescendentCrossJoinLevelMember(match.getMember1(),
                                match.getColumn(), match.getMember2());
                        } else if (match.isCrossJoin()) {
                            handler.addCrossJoinLevelMember(match.getMember1(), match.getColumn(),
                                match.getMember2());
                        } else {
                            handler.addLevelMember(match.getMember1(), match.getColumn(),
                                match.getMember2());
                        }
                        // add this info to the first slot.
                        if (first.right == null) {
                            first.right = match;
                        }
                        orderedRequest.add(Pair.of(request, match));
                        added = true;
                        break;
                    }
                }
            }
            if (!added) {
                List<Pair<R, ConsolidationUtils.LevelMatch>> currList
                    = new ArrayList<Pair<R, ConsolidationUtils.LevelMatch>>();
                currList.add(Pair.of(request, (ConsolidationUtils.LevelMatch) null));
                orderedRequests.add(currList);
            }

        }
        List<List<Pair<R, ConsolidationUtils.LevelMatch>>> singles
            = new ArrayList<List<Pair<R, ConsolidationUtils.LevelMatch>>>();
        List<Pair<R, ConsolidationUtils.LevelMatch>> currList
            = new ArrayList<Pair<R, ConsolidationUtils.LevelMatch>>();
        Iterator<List<Pair<R, LevelMatch>>> it = orderedRequests.iterator();
        while (it.hasNext()) {
            List<Pair<R, ConsolidationUtils.LevelMatch>> next = it.next();
            if (next.size() == 1) {
                it.remove();
                currList.add(next.get(0));
                if (currList.size() >= singleThreshold) {
                    singles.add(currList);
                    currList = new ArrayList<Pair<R, ConsolidationUtils.LevelMatch>>();
                }
            }
        }
        if (currList.size() > 0) {
            singles.add(currList);
        }
        return Pair.of(singles, orderedRequests);

    }

    /**
     * Container class to simplify processing and accessing relevant member and cross join info
     * associated with a constraint.
     */
    private static class ConstraintObject {
        List<Member> members1;
        List<CrossJoinArg> crossJoinArgs1;
        List<Member> members2;
        List<CrossJoinArg> crossJoinArgs2;

        public ConstraintObject(List<Member> members1, List<CrossJoinArg> crossJoinArgs1,
                                List<Member> members2, List<CrossJoinArg> crossJoinArgs2) {
            this.members1 = members1;
            this.crossJoinArgs1 = crossJoinArgs1;
            this.members2 = members2;
            this.crossJoinArgs2 = crossJoinArgs2;
        }
    }

    /**
     * Encapsulates information required to process two members that are considered a match.
     */
    public static class LevelMatch {

        private MondrianDef.Column column;
        private int matchIndex;
        private RolapMember member1;
        private RolapMember member2;
        private boolean crossJoin;
        private boolean descendantCrossJoin;

        public LevelMatch(MondrianDef.Column column, int matchIndex, RolapMember member1,
                          RolapMember member2, boolean crossJoin, boolean descendantCrossJoin) {
            this.column = column;
            this.matchIndex = matchIndex;
            this.member1 = member1;
            this.member2 = member2;
            this.crossJoin = crossJoin;
            this.descendantCrossJoin = descendantCrossJoin;
        }

        public MondrianDef.Column getColumn() {
            return column;
        }

        public int getMatchIndex() {
            return matchIndex;
        }

        public RolapMember getMember1() {
            return member1;
        }

        public RolapMember getMember2() {
            return member2;
        }

        public boolean isCrossJoin() {
            return crossJoin;
        }

        public boolean isDescendantCrossJoin() {
            return descendantCrossJoin;
        }

        public boolean matches(LevelMatch other) {
            return getColumn().equals(other.getColumn()) &&
                getMatchIndex() == other.getMatchIndex() &&
                isCrossJoin() == other.isCrossJoin() &&
                isDescendantCrossJoin() == other.isDescendantCrossJoin();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("LevelMatch{");
            sb.append("column=").append(column);
            sb.append(", matchIndex=").append(matchIndex);
            sb.append(", member1=").append(member1);
            sb.append(", member2=").append(member2);
            sb.append(", crossJoin=").append(crossJoin);
            sb.append(", descendantCrossJoin=").append(descendantCrossJoin);
            sb.append('}');
            return sb.toString();
        }
    }
}
