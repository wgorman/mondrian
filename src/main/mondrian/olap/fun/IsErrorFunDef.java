/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014 Pentaho Corporation..  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.AbstractBooleanCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianException;

public class IsErrorFunDef extends FunDefBase {
    static final ReflectiveMultiResolver FunctionResolver =
        new ReflectiveMultiResolver(
            "IsError",
            "IsError(<Value Expression>)",
            "Boolean value of TRUE if the value is an error; otherwise FALSE.",
            new String[] {"fbS", "fbn"},
            IsErrorFunDef.class);

    public IsErrorFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final Calc calc = compiler.compileScalar(call.getArg(0), true);
        return new AbstractBooleanCalc(call, new Calc[] {calc}) {
            public boolean evaluateBoolean(Evaluator evaluator) {
                Object o = calc.evaluate(evaluator);

                if (o instanceof Double) {
                    if (((Double)o).isInfinite() || ((Double) o).isNaN()) {
                        return true;
                    }
                }
                return o == null;
            }
        };
    }
}
// End IsErrorFunDef.java