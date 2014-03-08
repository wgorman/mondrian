/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import java.util.List;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;

/**
 * Definition of the <code>Subset</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class SubsetFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Subset",
            "Subset(<Set>, <Start>[, <Count>])",
            "Returns a subset of elements from a set.",
            new String[] {"fxxn", "fxxnn"},
            SubsetFunDef.class);

    public SubsetFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final IterCalc listCalc =
            compiler.compileIter(call.getArg(0));
        final IntegerCalc startCalc =
            compiler.compileInteger(call.getArg(1));
        final IntegerCalc countCalc =
            call.getArgCount() > 2
            ? compiler.compileInteger(call.getArg(2))
            : null;
        return new AbstractListCalc(
            call, new Calc[] {listCalc, startCalc, countCalc})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                ResolvedFunCall call = (ResolvedFunCall) exp;
                // Use a native evaluator, if more efficient.
                SchemaReader schemaReader = evaluator.getSchemaReader();
                NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                        call.getFunDef(), call.getArgs(), evaluator, this);
                if (nativeEvaluator != null) {
                    return (TupleList) nativeEvaluator.execute(
                        ResultStyle.ITERABLE);
                } else {
                    return evaluateListNonNative(evaluator);
                }
            }

            public TupleList evaluateListNonNative(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(false);
                    final TupleIterable iter = listCalc.evaluateIterable(evaluator);
                    final int start = startCalc.evaluateInteger(evaluator);
                    int end;
                    if (countCalc != null) {
                        final int count = countCalc.evaluateInteger(evaluator);
                        end = start + count;
                    } else {
                        end = Integer.MAX_VALUE;
                    }

                    if (start == 0 && end == Integer.MAX_VALUE) {
                        return TupleCollections.materialize( iter, false );
                    }
                    
                    if (start >= end || start < 0) {
                        return TupleCollections.emptyList(iter.getArity());
                    }
                    TupleIterator iterator = iter.tupleIterator();
                    int curr = 0;
                    TupleList list = TupleCollections.createList(iter.getArity());
                    while (iterator.hasNext() && curr < start) {
                        iterator.next();
                        curr++;
                    }
                    while (iterator.hasNext() && curr < end) {
                        // Add to a Tuple List 
                        List<Member> tuple = iterator.next();
                        list.add(tuple);
                        curr++;
                    }
                    return list;
                } finally {
                    evaluator.restore(savepoint);
                }
            }
        };
    }
}

// End SubsetFunDef.java
