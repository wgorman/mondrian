/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2014 Pentaho
// All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.IntegerCalc;
import mondrian.calc.IterCalc;
import mondrian.calc.ListCalc;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Literal;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.Validator;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;

public class AxisFunDef extends FunDefBase {

    static final AxisFunDef instance = new AxisFunDef();

    protected AxisFunDef() {
        super(
            "Axis",
            "Returns the set of tuples on a specified axis.",
            "fxn");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length != 1) {
            return null;
        }
        if (args[0] instanceof Literal) {
            int idx = ((Literal) args[0]).getIntValue();
            // failing validation at this point might just mean we're in
            // a calculated member and don't have axes yet
            if (validateAxis(idx, validator.getQuery())) {
                QueryAxis axis = validator.getQuery().getAxes()[idx];
                if (validator.isResolving(axis)) {
                    // evaluation loop
                    throw FunUtil.newEvalException(
                        this,
                        String.format(
                            "Axis(%d) was referenced before being resolved",
                            idx));
                }
                return getAxisType(axis, validator);
            }
        }
        return new SetType(new TupleType(new Type[] {MemberType.Unknown}));
    }

    private static Type getAxisType(QueryAxis axis, Validator validator) {
        axis.resolve(validator);
        return axis.getSet().getType();
    }
    private boolean validateAxis(int axisNbr, Query query) {
        return axisNbr >= 0 && axisNbr < query.getAxes().length;
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final IntegerCalc arg = compiler.compileInteger(call.getArg(0));
        return new AbstractListCalc(
            call,
            new Calc[] {arg})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                int axisIdx = arg.evaluateInteger(evaluator);
                Query query = evaluator.getQuery();
                // throw if invalid
                if (!AxisFunDef.this.validateAxis(axisIdx, query)) {
                    throw FunUtil.newEvalException(
                        AxisFunDef.this,
                       "Invalid axis number " + axisIdx);
                }
                if (axisIdx < query.axisCalcs.length) {
                    Calc axisCalc = evaluator.getQuery().axisCalcs[axisIdx];
                    if (axisCalc instanceof ListCalc) {
                        return ((ListCalc) axisCalc).evaluateList(evaluator);
                    } else if (axisCalc instanceof IterCalc) {
                        return TupleCollections.materialize(
                            ((IterCalc) axisCalc).evaluateIterable(evaluator),
                            false);
                    }
                }
                return null;
            }
        };
  }
}
// End AxisFunDef.java