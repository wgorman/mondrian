/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;

import java.util.ArrayList;
import java.util.List;

public class RolapNativeNonEmptyFunction extends RolapNativeSet {

    public RolapNativeNonEmptyFunction() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeNonEmptyFunction.get());
    }

    protected boolean restrictMemberTypes() {
        // can't really handle calculated measures
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

        // Native Args Validation
        // the first set
        // 0 - arguments, 1 - additional constraints
        List<CrossJoinArg[]> mainArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);
        if (failedCjArg(mainArgs)) {
            alertNonNative(evaluator, fun, args[0]);
            return null;
        }
        // we want the second arg to be added just as a crossjoin constraint
        boolean hasTwoArgs = args.length == 2;
        List<CrossJoinArg[]> extraArgs =
            hasTwoArgs
                ? crossJoinArgFactory().checkCrossJoinArg(evaluator, args[1])
                : null;
        if (hasTwoArgs
            && failedCjArg(extraArgs))
        {
            alertNonNative(evaluator, fun, args[1]);
            return null;
            // TODO: if measures are present find if they can be separated and
            // filtered afterwards
        }

        // what should end up in the select
        final CrossJoinArg[] returnArgs = mainArgs.get(0);
        // what will be in the constraint
        final CrossJoinArg[] constraintArgs =
            getConstraintArgs(mainArgs, extraArgs);
        // crossjoin members that will override context
        final CrossJoinArg[] cjArgs =
            hasTwoArgs
                ? Util.appendArrays(returnArgs, extraArgs.get(0))
                : returnArgs;

        SchemaReader schemaReader = evaluator.getSchemaReader();

        final int savepoint = evaluator.savepoint();
        try {
            overrideContext(evaluator, cjArgs, null);
            NonEmptyFunctionConstraint constraint =
                new NonEmptyFunctionConstraint(
                    constraintArgs, evaluator, restrictMemberTypes());

            SetEvaluator nativeEvaluator =
                new SetEvaluator(returnArgs, schemaReader, constraint);
            RolapUtil.SQL_LOGGER.debug("NonEmpty() going native");
            return nativeEvaluator;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private static boolean failedCjArg(List<CrossJoinArg[]> args) {
        return args == null || args.isEmpty() || args.get(0) == null;
    }

    private CrossJoinArg[] getConstraintArgs(
        List<CrossJoinArg[]> firstSet, List<CrossJoinArg[]> secondSet)
    {
        //get everything into one array
        final CrossJoinArg[] empty = new CrossJoinArg[0];
        return Util.appendArrays(
            firstSet.get(0),
            (firstSet.size() > 1) ? firstSet.get(1) : empty,
            (secondSet != null) ? secondSet.get(0) : empty,
            (secondSet != null && secondSet.size() > 1)
                ? secondSet.get(1)
                : empty);
    }

    private static void alertNonNative(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp offendingArg)
    {
        if (!evaluator.getQuery().shouldAlertForNonNative(fun)) {
            return;
        }
        RolapUtil.alertNonNative(
            fun.getName(),
            "set argument " + offendingArg.toString());
    }

    static class NonEmptyFunctionConstraint extends SetConstraint {

        NonEmptyFunctionConstraint(
            CrossJoinArg[] args, RolapEvaluator evaluator, boolean restrict)
        {
            super(args, evaluator, restrict);
        }

        @Override
        public void constrainExtraLevels(
            RolapCube baseCube,
            BitKey levelBitKey)
        {
            super.constrainExtraLevels(baseCube, levelBitKey);
            for (CrossJoinArg arg : args) {
                if (arg instanceof DescendantsCrossJoinArg
                    || arg instanceof MemberListCrossJoinArg)
                {
                    final RolapLevel level = arg.getLevel();
                    if (level != null && !level.isAll()) {
                        RolapStar.Column column =
                            ((RolapCubeLevel)level)
                                .getBaseStarKeyColumn(baseCube);
                        levelBitKey.set(column.getBitPosition());
                    }
                }
            }
        }

        @Override
        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            //  // we're "special"
            //  key.add(this.getClass());
            key.add(super.getCacheKey());
            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.add(
                    ((RolapEvaluator)this.getEvaluator())
                    .getSlicerMembers());
            }
            return key;
        }
    }
}
// End RolapNativeNonEmptyFunction.java