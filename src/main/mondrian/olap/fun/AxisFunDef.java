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
            validateAxis(idx, validator.getQuery());
            QueryAxis axis = validator.getQuery().getAxes()[idx];
            if (validator.isResolving(axis)) {
                throw FunUtil.newEvalException(
                    this,
                    String.format(
                        "Axis(%d) was referenced before being resolved", idx));
            }
            return getAxisType(axis, validator);
        }
        // TODO: no loop detection in this case, should we disallow it or
        // evaluate the expression at this point?
        return new SetType(new TupleType(new Type[] {}));
    }

    private static Type getAxisType(QueryAxis axis, Validator validator) {
        //  Exp axisExp = axis.getSet().clone();
        //  axisExp = validator.validate(axisExp, false);
        //  return axisExp.getType();
        axis.resolve(validator);
        return axis.getSet().getType();
    }
    private void validateAxis(int axisNbr, Query query) {
        if (axisNbr < 0 || axisNbr >= query.getAxes().length) {
            throw FunUtil.newEvalException(
                this,
               "Inavlid axis number " + axisNbr);
        }
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final IntegerCalc arg = compiler.compileInteger(call.getArg(0));
        //  int axisIdx = arg.evaluateInteger(compiler.getEvaluator());
        //  return compiler.getEvaluator().getQuery().axisCalcs[axisIdx];
        return new AbstractListCalc(
            call,
            new Calc[] {arg})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                int axisIdx = arg.evaluateInteger(evaluator);
                Query query = evaluator.getQuery();
                // throw if invalid
                AxisFunDef.this.validateAxis(axisIdx, query);
                if (axisIdx < query.axisCalcs.length) {
                    // QueryAxis axis = axes[axisIdx];
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