/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleCursor;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.QueryAxis;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapEvaluator;

import java.util.ArrayList;
import java.util.List;

public class NonEmptyFunDef extends FunDefBase {
    static final MultiResolver NonEmptyResolver =
        new MultiResolver(
            "NonEmpty",
            "NonEmpty(<Set>[, <Set>])",
            "Returns the tuples in the first set that are non-empty when evaluated across the tuples in the second set, or in the context of the current coordinates if not provided.",
            new String[]{"fxx", "fxxx"})
        {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                return new NonEmptyFunDef(dummyFunDef);
            }
        };

    protected NonEmptyFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc arg1Calc = compiler.compileList(call.getArg(0));
        final ListCalc arg2Calc = call.getArgCount() > 1
            ? compiler.compileList(call.getArg(1))
            : null;
        // determine overridden types
        List<MemberType> typeList = new ArrayList<MemberType>();
        for (Exp arg : call.getArgs()) {
            CrossJoinFunDef.addTypes(arg.getType(), typeList);
        }
        MemberType[] types =
            typeList.toArray(new MemberType[typeList.size()]);
        // must form valid tuples
        TupleType.checkHierarchies(types);
        // for hierarchy usage check
        final TupleType argsType = new TupleType(types);
        QueryAxis slicer = compiler.getValidator().getQuery().getSlicerAxis();
        final Type slicerType =
            (slicer != null)
                ? slicer.getSet().getType()
                : null;

        //  boolean hasMeasures = false;
        //  for (MemberType type : types) {
        //      if (type.getHierarchy().getDimension().isMeasures()) {
        //          hasMeasures = true;
        //          break;
        //      }
        //  }
        //  final boolean mustConstrainMeasures = !hasMeasures;

        return new AbstractListCalc(
            call, new Calc[] {arg1Calc, arg2Calc}, false)
        {
            @Override
            public boolean dependsOn(Hierarchy hierarchy) {
                boolean depends = super.dependsOn(hierarchy);
                // we depend on the slicer if not overrding it
                // if we don't report this SimplifyEvaluator
                // will reset the context
                return depends
                    || (slicerType != null
                        && slicerType.usesHierarchy(hierarchy, false));
            }

            public TupleList evaluateList(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                 try {
                    // empty args can be stripped at the source for some cases
                    // when is it ok to set nonempty?

                    // remove slicer members overridden by args
                    for (Member member
                        : ((RolapEvaluator) evaluator).getSlicerMembers())
                    {
                        if (argsType.usesHierarchy(
                                member.getHierarchy(), true))
                        {
                            evaluator.setContext(
                                member.getHierarchy().getAllMember());
                        }
                    }

                    // attempt native
                    NativeEvaluator nativeEvaluator =
                        evaluator.getSchemaReader().getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        evaluator.restore(savepoint);
                        return
                            (TupleList) nativeEvaluator.execute(
                                ResultStyle.LIST);
                    }

                    final TupleIterable setIterable =
                        arg1Calc.evaluateIterable(evaluator);
                    TupleIterable auxIterable =
                        arg2Calc != null
                            ? arg2Calc.evaluateIterable(evaluator)
                            : null;

                    return (auxIterable == null)
                        ? nonEmpty(evaluator, setIterable)
                        : nonEmpty(evaluator, setIterable, auxIterable);
                } finally {
                    evaluator.restore(savepoint);
                }
            }
        };
    }

    public static TupleList nonEmpty(Evaluator eval, TupleIterable main) {
        TupleList result = TupleCollections.createList(main.getArity());
        final Member[] currentMembers = new Member[main.getArity()];
        final TupleCursor mainCursor = main.tupleCursor();
        while (mainCursor.forward()) {
            mainCursor.currentToArray(currentMembers, 0);
            eval.setContext(currentMembers);
            if (eval.evaluateCurrent() != null) {
                result.add(mainCursor.current());
            }
        }
        return result;
    }

    public static TupleList nonEmpty(
        Evaluator eval,
        TupleIterable main, TupleIterable aux)
    {
        final int arityMain = main.getArity();
        final int arity = arityMain + aux.getArity();
        TupleList result =
            TupleCollections.createList(arityMain);
        final TupleCursor mainCursor = main.tupleCursor();
        TupleCursor auxCursor = null;
        final Member[] currentMembers = new Member[arity];

        // "crossjoin" iterables and check for nonemptyness
        // only the first tuple is returned
        while (mainCursor.forward()) {
            boolean isNonEmpty = false;
            auxCursor = aux.tupleCursor();
            mainCursor.currentToArray(currentMembers, 0);
            inner : while (auxCursor.forward()) {
                auxCursor.currentToArray(currentMembers, arityMain);
                eval.setContext(currentMembers);
                if (eval.evaluateCurrent() != null) {
                    isNonEmpty = true;
                    break inner;
                }
            }
            if (isNonEmpty) {
                result.add(mainCursor.current());
            }
        }
        return result;
    }

}
// End NonEmptyFunDef.java