/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2014 Pentaho Corporation..  All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.EmptyType;
import mondrian.rolap.RolapUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the <code>EXISTS</code> MDX function.
 *
 * @author kvu
 * @since Mar 23, 2008
 */
class ExistsFunDef extends FunDefBase
{
    static final Resolver resolver =
        new ReflectiveMultiResolver(
            "Exists",
            "Exists(<Set1>, <Set2> [, <Cube Name>])",
            "Returns the the set of tuples of the first set that exist with one or more tuples of the second set."
            + " If <Cube Name> is specified only elements with entries in that cube's fact table will be returned.",
            new String[] {"fxxx", "fxxxS", "fxxeS"},
            ExistsFunDef.class);

    public ExistsFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        // Set2 arg can be empty iff there is a cubeName
        final ListCalc listCalc2 =
            (call.getArg(1).getType() instanceof EmptyType)
                ? null
                : compiler.compileList(call.getArg(1));
        final StringCalc cubeNameCalc = call.getArgCount() > 2
            ? compiler.compileString(call.getArg(2))
            : null;
        // signature enforces this
        assert listCalc2 != null || cubeNameCalc != null;

        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public TupleList evaluateList(Evaluator evaluator) {
                String cubeName = (cubeNameCalc != null)
                    ? cubeNameCalc.evaluateString(evaluator)
                    : null;
                // it's not enough to be null, the element must not exist in the
                //  fact table to be considered empty here.
                // we need to break into rolap namespace and use the fact count
                final Member targetMeasure = (cubeName != null)
                    ? RolapUtil.getFactCountMeasure(evaluator, cubeName, true)
                    : null;

                TupleList leftTuples = listCalc1.evaluateList(evaluator);
                if (leftTuples.isEmpty()) {
                    return TupleCollections.emptyList(leftTuples.getArity());
                }

                TupleList rightTuples = (listCalc2 != null)
                    ? listCalc2.evaluateList(evaluator)
                    : null;
                if (rightTuples != null && rightTuples.isEmpty()) {
                    return TupleCollections.emptyList(leftTuples.getArity());
                }
                TupleList result =
                    TupleCollections.createList(leftTuples.getArity());

                if (rightTuples == null) {
                    // just filter by measure
                    for (List<Member> leftTuple : leftTuples) {
                        if (isNotEmptyForFactCountMeasure(
                                leftTuple, evaluator, targetMeasure))
                        {
                          result.add(leftTuple);
                        }
                    }
                    return result;
                }

                // check for second set
                List<Hierarchy> leftDims = getHierarchies(leftTuples.get(0));
                List<Hierarchy> rightDims = getHierarchies(rightTuples.get(0));

                leftLoop:
                for (List<Member> leftTuple : leftTuples) {
                    for (List<Member> rightTuple : rightTuples) {
                        if (existsInTuple(leftTuple, rightTuple,
                            leftDims, rightDims, null))
                        {
                            if (isNotEmptyForFactCountMeasure(
                                leftTuple, evaluator, targetMeasure))
                            {
                                result.add(leftTuple);
                            }
                            continue leftLoop;
                        }
                    }
                }
                return result;
            }
        };
    }

    private boolean isNotEmptyForFactCountMeasure(
        List<Member> tupleElement,
        Evaluator eval,
        Member factCountMeasure)
    {
        if (factCountMeasure == null) {
            return true;
        }
        eval.setContext(tupleElement);
        eval.setContext(factCountMeasure);
        Object o = eval.evaluateCurrent();
        return o != null
            && !(o instanceof Number && ((Number) o).intValue() == 0);
    }

    private static List<Hierarchy> getHierarchies(final List<Member> members)
    {
        List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();
        for (Member member : members) {
            hierarchies.add(member.getHierarchy());
        }
        return hierarchies;
    }

}

// End ExistsFunDef.java
