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

import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.SqlQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
//TODO testing            getContextArgs(cjArgs, evaluator, restrictMemberTypes());
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
     * FIXME: change to use dimensions
     * @param cjArgs
     * @param evaluator
     * @param restrictMemberTypes
     * @return
     */
    public static CrossJoinArg[] getContextArgs(
        CrossJoinArg[] cjArgs,
        RolapEvaluator evaluator,
        boolean restrictMemberTypes)
    {
        ArrayList<CrossJoinArg> contextPredicates =
            new ArrayList<CrossJoinArg>();
        for (CrossJoinArg cjArg : cjArgs) {
            // get each relevant context member
            RolapLevel level = cjArg.getLevel();
            if (level != null) {
                RolapHierarchy hierarchy = level.getHierarchy();
                RolapMember member = evaluator.getContext(hierarchy);
                if (!member.isAll()) {
                    CrossJoinArg predicate =
                        MemberListCrossJoinArg.create(
                            evaluator,
                            Collections.singletonList(member),
                            restrictMemberTypes, false);
                    if (predicate == null){ 
                        return null;
                    }
                    contextPredicates.add(predicate);
                }
            }
        }
        return contextPredicates.toArray(
            new CrossJoinArg[contextPredicates.size()]);
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

    private static class ExistingConstraint extends SetConstraint {
        // only need the context
        ExistingConstraint(CrossJoinArg[] args, RolapEvaluator evaluator) {
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

}
// End RolapNativeExisting.java