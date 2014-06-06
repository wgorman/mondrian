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

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
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
//            if (args[0] instanceof ResolvedFunCall) {
//                // could be a sql-pushable that isn't a cjarg,
//                // we should be able to work with that
//                NativeEvaluator delegatingEvaluator =
//                  getDelegatingEvaluator(evaluator, (ResolvedFunCall) args[0]);
//                if (delegatingEvaluator != null) {
//                    return delegatingEvaluator;
//                }
//            }
            alertNonNative(evaluator, fun, args[0]);
            return null;
        }
        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }
        final CrossJoinArg[] contextPredicateArgs =
            getContextArgs(cjArgs, evaluator, restrictMemberTypes());
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
//        LOGGER.debug("native EXISTING");
//        RolapEvaluator setEvaluator = evaluator.push();
//        overrideContext(setEvaluator, cjArgs, null);
//        return new SetEvaluator(
//            cjArgs,
//            setEvaluator.getSchemaReader(),
//            constraint);
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

//    public NativeEvaluator getDelegatingEvaluator(
//        RolapEvaluator evaluator,
//        ResolvedFunCall innerCall)
//    {
//        NativeEvaluator eval = evaluator.getSchemaReader().getSchema().getNativeRegistry().createEvaluator(
//            evaluator, innerCall.getFunDef(), innerCall.getArgs());
//        if (eval instanceof SetEvaluator) {
//            SetEvaluator setEval = (SetEvaluator) eval;
//            if (setEval.getArgs() != null
//                && setEval.getConstraint() instanceof SetConstraint)
//            {
//                SetConstraint setConstraint =
//                    (SetConstraint) setEval.getConstraint();
//                final CrossJoinArg[] contextPredicateArgs =
//                    getContextArgs(
//                        setEval.getArgs(),
//                        evaluator,
//                        restrictMemberTypes());
//                if (contextPredicateArgs != null) {
//                    DelegatingExistingConstraint constraint =
//                        new DelegatingExistingConstraint(
//                            setEval.getArgs(),
//                            contextPredicateArgs,
//                            evaluator,
//                            setConstraint, true);
//                    LOGGER.debug("delegate EXISTING");
//                    return createNativeEvaluator(evaluator, setEval.getArgs(), constraint);
//                }
//            }
//        }
//        return null;
//    }

    /**
     * 
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

//    static class DelegatingExistingConstraint extends SetConstraint {
//        private SetConstraint delegatingConstraint;
//        private CrossJoinArg[] contextPredicates;
//
//        DelegatingExistingConstraint(
//            CrossJoinArg[] cjArgs,
//            CrossJoinArg[] context,
//            RolapEvaluator evaluator,
//            SetConstraint inner,
//            boolean strict)
//        {
//            super(cjArgs, evaluator, strict);
//            assert inner != null;
//            this.delegatingConstraint = inner;
//            this.contextPredicates = context;
//        }
//        @Override
//        public void addConstraint(
//            SqlQuery sqlQuery,
//            RolapCube baseCube,
//            AggStar aggStar)
//        {
//            delegatingConstraint.addConstraint(sqlQuery, baseCube, aggStar);
//            for (CrossJoinArg arg : contextPredicates) {
//                arg.addConstraint(sqlQuery, baseCube, aggStar);
//            }
//        }
//
//        @Override
//        protected boolean isJoinRequired() {
//            return delegatingConstraint.isJoinRequired();
//        }
//
//        @Override
//        public Object getCacheKey() {
//            ArrayList<Object> key = new ArrayList<Object>(2); 
//            key.add(delegatingConstraint.getCacheKey());
//            key.add(args);
//            return key;
//        }
//    }

}
// End RolapNativeExisting.java