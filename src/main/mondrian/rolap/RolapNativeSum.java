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
import java.util.Set;
import java.util.TreeSet;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Util;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.MemberListCrossJoinArg;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.Dialect;

/**
 * Computes a Sum(set) in SQL.  Should only go native if certain conditions are met,
 * including a prediction that the tuples to sum will be of a high cardinality. 
 *
 * @author Will Gorman (wgorman@pentaho.com)
 */
public class RolapNativeSum extends RolapNativeSet {

    public RolapNativeSum() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeSum.get());
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
        if (!SqlContextConstraint.isValidContext(
                evaluator, false, new Level[]{}, restrictMemberTypes()))
        {
            return null;
        }
        // is this "Sum(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Sum".equalsIgnoreCase(funName)) {
            return null;
        }

        if (!(args[0] instanceof ResolvedFunCall)) {
            return null;
        }

        // Check to see if evaluator contains a calculated member that can't be
        // expanded.  This is necessary due to the SqlConstraintsUtils.
        // addContextConstraint()
        // method which gets called when generating the native SQL.
        if (SqlConstraintUtils.containsCalculatedMember(
                evaluator.getNonAllMembers(), true))
        {
            return null;
        }

        ResolvedFunCall call = (ResolvedFunCall)args[0];

        if (call.getFunDef().getName().equals("Cache")) {
            if (call.getArg( 0 ) instanceof ResolvedFunCall) {
                call = (ResolvedFunCall)call.getArg(0);
            } else {
                return null;
            }
        }

        // TODO: Note that this native evaluator won't work against tuples that are mapped to
        // security, because the security is usually applied after native evaluation.
        // Before this can be considered valid we need to test in that usecase and 
        // either push down the security into SQL or not natively evaluate.

        // TODO: Check to see if addRoleAccessConstraints() natively evaluates roles, if that is
        // always applicable or not.  See why there was a cleansing step at the end of native tuple
        // evaluation.

        // Determine the measure in play for the Sum() function
        Member measure = null;
        if (args.length == 1) {
            // get current measure
            measure = evaluator.getContext(evaluator.getCube().getMeasuresHierarchy()); 
        } else {
            // call findMeasure
            Set<Member> measures = new TreeSet<Member>();
            findMeasure(args[1], measures);
            if (measures.size() == 1) {
                measure = measures.iterator().next();
            } else {
                // No Measure Found
                // TODO: Support [Measures].CurrentMember
                return null;
            }
        }

        if (measure.isCalculated()) {
            return null;
        }

        // First check to see if the function Sum is wrapping is natively evaluated.  If so, 
        // just minor changes are needed to be made to the evaluator.
        SetEvaluator eval = (SetEvaluator)evaluator.getSchemaReader().getSchema().getNativeRegistry().createEvaluator(
            evaluator, call.getFunDef(), call.getArgs());

        if (eval != null) {
            // TopCount or Filter measure does not share the same cube as the current measure
            if (eval.getMeasure() != null && !areFromSameCube(eval.getMeasure(), measure)) {
              return null;
            }

            // create a delegating sum constraint
            int cardinality = estimateCardinality(eval.getArgs());
            if (cardinality <= 1) { // change to a reasonable cardinality limit.
              return null;
            }
            SumConstraint sumConstraint = new SumConstraint(eval.getConstraint(), new CrossJoinArg[0], evaluator, false, measure);
            eval.setConstraint(sumConstraint);
        } else {
            List<CrossJoinArg[]> allArgs =
                crossJoinArgFactory().checkCrossJoinArg(evaluator, call, false, false);
            if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
                return null;
            }
            int cardinality = estimateCardinality(allArgs.get(0));
            // TODO: change to a reasonable default, configurable cardinality limit.
            if (cardinality < 100) {
              return null;
            }
            final int savepoint = evaluator.savepoint();
            try {
                overrideContext(evaluator, allArgs.get(0), null);
                SumConstraint sumConstraint = new SumConstraint(null,
                    Util.appendArrays(allArgs.get(0), allArgs.size() == 2 ? allArgs.get(1) : new CrossJoinArg[0]),
                    evaluator, false, measure); 
                eval = new SetEvaluator(allArgs.get(0), evaluator.getSchemaReader(), sumConstraint);
            } finally {
                evaluator.restore(savepoint);
            }
        }

        if (eval != null) {
            LOGGER.debug("using native sum");  
        }
        return eval;
    }

    private boolean areFromSameCube(Member m1, Member m2) {
        if (!(m1 instanceof RolapStoredMeasure) 
            || !(m2 instanceof RolapStoredMeasure)) 
        {
          return false;
        }
        return getStar((RolapStoredMeasure)m1) 
                == getStar((RolapStoredMeasure)m2);
    }

    private RolapStar getStar(RolapStoredMeasure m) {
      return ((RolapStar.Measure) m.getStarMeasure()).getStar();
    }

    /**
     * This method determines an estimate cardinality of the related expressions.
     */
    private int estimateCardinality(CrossJoinArg[] allArgs) {
        int cardinality = 0;
        for (CrossJoinArg arg : allArgs) {
            // TODO: implement a "CrossJoinArg.estimateCardinality()" method instead
            if (arg instanceof MemberListCrossJoinArg) {
                cardinality += arg.getMembers().size();
            } else {
                // need to handle all use cases, for now only skip the
                // obvious single member native sums()
                // TODO: extract the cardinality appropriately from the arg
                cardinality += 10000;
            }
        }
        return cardinality;
    }

    /**
     * Extracts the stored measures referenced in an expression
     *
     * @param exp expression
     * @param baseCubes set of base cubes
     */
    private static void findMeasure(
        Exp exp,
        Set<Member> foundMeasure)
    {
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            Member member = memberExpr.getMember();
            if (member instanceof RolapStoredMeasure) {
                if (!foundMeasure.contains( member )) {
                    foundMeasure.add(member);
                }
            } else if (member instanceof RolapCalculatedMember) {
                if (!foundMeasure.contains(member)) {
                    foundMeasure.add(member);
                    findMeasure(member.getExpression(),foundMeasure);
                }
            }
        } else if (exp instanceof ResolvedFunCall) {
            ResolvedFunCall funCall = (ResolvedFunCall) exp;
            Exp [] args = funCall.getArgs();
            for (Exp arg : args) {
                findMeasure(arg, foundMeasure);
            }
        }
    }

    static class SumConstraint extends SetConstraint {

        Member measure;
        TupleConstraint delegatingConstraint;

        SumConstraint(TupleConstraint delegatingConstraint,
            CrossJoinArg[] args, RolapEvaluator evaluator, boolean restrict, Member measure)
        {
            super(args, evaluator, restrict);
            this.measure = measure;
            this.delegatingConstraint = delegatingConstraint;
        }

        @Override
        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (delegatingConstraint != null) {
                delegatingConstraint.addConstraint( sqlQuery, baseCube, aggStar );
            } else {
                super.addConstraint( sqlQuery, baseCube, aggStar );
            }
            // Add the measure to the query as "m1", make sure the fact table is also present.
            addMeasureFact(measure, aggStar, sqlQuery);
            sqlQuery.addSelect( getMeasureExpression(measure, aggStar, sqlQuery), null, "m1");
        }

        private void addMeasureFact(Member member, AggStar aggStar, SqlQuery sqlQuery) {
            RolapStoredMeasure measure = (RolapStoredMeasure) member;
            // this is only necessary if the sum itself is empty of tuples
            if (aggStar != null) {
                aggStar.getFactTable().addToFrom( sqlQuery, false, false);
            } else {
                measure.getCube().getStar().getFactTable().addToFrom( sqlQuery,  false,  false);
            }
        }

        private String getMeasureExpression(Member member, AggStar aggStar, SqlQuery sqlQuery) {
            RolapStoredMeasure measure = (RolapStoredMeasure) member;
            String exprInner;
            // Use aggregate table to create condition if available
            if (aggStar != null
                && measure.getStarMeasure() instanceof RolapStar.Column)
            {
                RolapStar.Column column =
                    (RolapStar.Column) measure.getStarMeasure();
                int bitPos = column.getBitPosition();
                AggStar.Table.Column aggColumn = aggStar.lookupColumn(bitPos);
                exprInner = aggColumn.generateExprString(sqlQuery);
            } else {
                exprInner =
                    measure.getMondrianDefExpression().getExpression(sqlQuery);
            }

            String expr = measure.getAggregator().getExpression(exprInner);
            if (sqlQuery.getDialect().getDatabaseProduct().getFamily()
                == Dialect.DatabaseProduct.DB2)
            {
                expr = "FLOAT(" + expr + ")";
            }
            return expr;
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

        protected boolean isJoinRequired() {
            // Native Sum always join to the fact table.
            return true;
        }

        @Override
        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            //  // we're "special"
            key.add(this.getClass());
            key.add(measure);
            if (delegatingConstraint != null) {
                key.add(delegatingConstraint.getCacheKey());
            } else {
                key.add(super.getCacheKey());              
                if (this.getEvaluator() instanceof RolapEvaluator) {
                    key.add(
                        ((RolapEvaluator)this.getEvaluator())
                        .getSlicerMembers());
                }
            }
            return key;
        }

        @Override
        public void addLevelConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar,
            RolapLevel level) 
        {
            if (delegatingConstraint != null) { 
                delegatingConstraint.addLevelConstraint(sqlQuery, baseCube, aggStar, level);
            } else {
                super.addLevelConstraint(sqlQuery, baseCube, aggStar, level);
            }
        }

        @Override
        public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
            if (delegatingConstraint != null) { 
                return delegatingConstraint.getMemberChildrenConstraint(parent);
            } else {
                return super.getMemberChildrenConstraint(parent);
            }
        }

        /**
         * @return the evaluator currently associated with the constraint; null
         * if there is no associated evaluator
         */
        public Evaluator getEvaluator() {
            if (delegatingConstraint != null) {
                return delegatingConstraint.getEvaluator();
            } else {
                return super.getEvaluator();
            }
        }
    }
}

// End RolapNativeSum.java
