/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;

/**
 * Computes a Subset in SQL.
 *
 * @author Will Gorman <wgorman@pentaho.com>
 * @since Mar 6, 2014
  */
public class RolapNativeSubset extends RolapNativeSet {

    public RolapNativeSubset() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeSubset.get());
    }

    /**
     * This constraint can act in two ways.  Either as a self contained
     * constraint, or as a wrapper around an existing native constraint.
     */
    static class SubsetConstraint extends SetConstraint {
        Integer start;
        Integer count;
        SetConstraint parentConstraint;

        public SubsetConstraint(
            Integer start,
            Integer count,
            CrossJoinArg[] args, 
            RolapEvaluator evaluator,
            SetConstraint parentConstraint
            )
        {
            super(args, evaluator, true);
            this.start = start;
            this.count = count;
            this.parentConstraint = parentConstraint;
        }

        /**
         * {@inheritDoc}
         *
         * <p>Subset doesn't require a join to the fact table.
         */
        protected boolean isJoinRequired() {
            if (parentConstraint != null) {
                return parentConstraint.isJoinRequired();
            } else {
                return super.isJoinRequired();
            }
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (start != null) {
                sqlQuery.setOffset(start);
            }
            if (count != null) {
              sqlQuery.setLimit(count);
            }
            if (parentConstraint != null) {
                parentConstraint.addConstraint( sqlQuery, baseCube, aggStar);
            } else {
                super.addConstraint(sqlQuery, baseCube, aggStar);
            }
        }

        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            if (parentConstraint != null) {
                key.add(parentConstraint.getCacheKey());
            } else {
                key.add(super.getCacheKey());
            }
            key.add(start);
            key.add(count);

            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.add(
                    ((RolapEvaluator)this.getEvaluator())
                    .getSlicerMembers());
            }
            return key;
        }

        public MemberChildrenConstraint getMemberChildrenConstraint(
            RolapMember parent)
        {
            if (parentConstraint != null) {
                return parentConstraint.getMemberChildrenConstraint(parent);
            } else {
                return super.getMemberChildrenConstraint(parent);
            }
        }

        public void constrainExtraLevels(
            RolapCube baseCube,
            BitKey levelBitKey)
        {
            if (parentConstraint != null) {
                parentConstraint.constrainExtraLevels(baseCube, levelBitKey);
            } else {
                super.constrainExtraLevels(baseCube, levelBitKey);
            }
        }
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
        if (!SubsetConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }

        // is this "Subset(<set>, <start>, [<count>])"
        String funName = fun.getName();
        if (!"Subset".equalsIgnoreCase(funName)) {
            return null;
        }
        if (args.length < 2 || args.length > 3) {
            return null;
        }

        // extract start
        if (!(args[1] instanceof Literal)) {
            return null;
        }
        int start = ((Literal) args[1]).getIntValue();

        Integer count = null;
        if (args.length == 3) {
            if (!(args[2] instanceof Literal)) {
                return null;
            }
            count = ((Literal) args[2]).getIntValue();
        }

        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeFilter");

        if (!sqlQuery.getDialect().supportsLimitAndOffset()) {
          return null;
        }

        // first see if subset wraps another native evaluation (other than count and sum)

        SetEvaluator eval = null;
        if (args[0] instanceof ResolvedFunCall) {
            ResolvedFunCall call = (ResolvedFunCall)args[0];
            if (call.getFunDef().getName().equals("Cache")) {
                if (call.getArg( 0 ) instanceof ResolvedFunCall) {
                    call = (ResolvedFunCall)call.getArg(0);
                } else {
                    return null;
                }
            }
            eval = (SetEvaluator)evaluator.getSchemaReader().getSchema().getNativeRegistry().createEvaluator(
                evaluator, call.getFunDef(), call.getArgs());
        }
        if (eval == null) {
            // extract the set expression
            List<CrossJoinArg[]> allArgs =
                crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

            // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
            // array is the CrossJoin dimensions.  The second array, if any,
            // contains additional constraints on the dimensions. If either the list
            // or the first array is null, then native cross join is not feasible.
            if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
                return null;
            }

            CrossJoinArg[] cjArgs = allArgs.get(0);
            if (isPreferInterpreter(cjArgs, false)) {
                return null;
            }

            final int savepoint = evaluator.savepoint();
            try {
                overrideContext(evaluator, cjArgs, null);
                CrossJoinArg[] predicateArgs = null;
                if (allArgs.size() == 2) {
                    predicateArgs = allArgs.get(1);
                }
                CrossJoinArg[] combinedArgs;
                if (predicateArgs != null) {
                    // Combined the CJ and the additional predicate args
                    // to form the TupleConstraint.
                    combinedArgs =
                            Util.appendArrays(cjArgs, predicateArgs);
                } else {
                    combinedArgs = cjArgs;
                }

                TupleConstraint constraint =
                    new SubsetConstraint(
                        start, count, combinedArgs, evaluator, null);
                SetEvaluator sev =
                    new SetEvaluator(cjArgs, schemaReader, constraint);
                sev.setMaxRows(count);
                LOGGER.debug("using native subset");
                return sev;
            } finally {
                evaluator.restore(savepoint);
            }
        } else {
            // if subset wraps another native function, add start and count to the constraint.
            TupleConstraint constraint =
                new SubsetConstraint(
                    start, count, null, evaluator, (SetConstraint)eval.getConstraint());
            eval.setConstraint(constraint);
            LOGGER.debug("using native subset");
            return eval;
        }
    }
}

// End RolapNativeSubset.java
