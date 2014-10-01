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
import mondrian.calc.IterCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;
import mondrian.rolap.ManyToManyUtil;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapNativeExisting;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Existing keyword limits a set to what exists within the current context, ie
 * as if context members of the same dimension as the set were in the slicer.
 */
public class ExistingFunDef extends FunDefBase {

    static final ExistingFunDef instance = new ExistingFunDef();
    private static final Logger LOGGER =
        Logger.getLogger(ExistingFunDef.class);

    protected ExistingFunDef() {
      super(
          "Existing",
          "Existing <Set>",
          "Forces the set to be evaluated within the current context.",
          "Pxx");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        return args[0].getType();
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final IterCalc setArg = compiler.compileIter(call.getArg(0));
         final Type myType = call.getArg(0).getType();

        return new AbstractListCalc(call, new Calc[] {setArg}) {
            public boolean dependsOn(Hierarchy hierarchy) {
                // TODO: REVIEW definitely arg in SetType#usesHierarchy
                // Note, this is used by native evaluation.
                // otherwise the native evaluator will override the current
                // context to the default.
                boolean argsDepend = super.dependsOn(hierarchy);
                return argsDepend
                    || myType.usesDimension(hierarchy.getDimension(), false);
            }

            public TupleList evaluateList(Evaluator evaluator) {
                RolapEvaluator manyToManyEval =
                    ManyToManyUtil.getManyToManyEvaluator(
                        (RolapEvaluator)evaluator);
                NativeEvaluator nativeEvaluator =
                    evaluator.getSchemaReader().getNativeSetEvaluator(
                        call.getFunDef(),
                        call.getArgs(),
                        manyToManyEval,
                        this);
                if (nativeEvaluator != null) {
                    return (TupleList)
                        nativeEvaluator.execute(ResultStyle.LIST);
                } else {
                    TupleIterable setTuples =
                        setArg.evaluateIterable(evaluator);
                    TupleList result =
                        TupleCollections.createList(setTuples.getArity());
                    List<Member> contextMembers =
                        Arrays.asList(evaluator.getMembers());

                    List<Hierarchy> argDims = null;
                    List<Hierarchy> contextDims =
                        getHierarchies(contextMembers, false);

                    for (List<Member> tuple : setTuples) {
                        if (argDims == null) {
                            argDims = getHierarchies(tuple, false);
                        }
                        if (existsInTuple(tuple, contextMembers,
                            argDims, contextDims, evaluator))
                        {
                            result.add(tuple);
                        }
                    }
                    // include different hierarchies from the same dimension
                    return postFilterDiffHierarchies(
                        (RolapEvaluator) evaluator,
                        argDims,
                        getHierarchies(contextMembers, true),
                        result,
                        new UnaryTupleList(contextMembers));
                }
            }
        };
    }

    private static List<Hierarchy> getHierarchies(
        final List<Member> members,
        boolean removeAlls)
    {
        List<Hierarchy> hierarchies = new ArrayList<Hierarchy>(members.size());
        for (Member member : members) {
            if (!removeAlls || !member.isAll()) {
                hierarchies.add(member.getHierarchy());
            }
        }
        return hierarchies;
    }

    /**
     * Members can be constrained by members of a different hierarchy in the
     * same dimension.
     */
    private static TupleList postFilterDiffHierarchies(
        RolapEvaluator evaluator,
        List<Hierarchy> leftHierarchies,
        List<Hierarchy> rightHierarchies,
        TupleList leftTuples,
        TupleList contextMembers)
    {
        Map<Dimension, Set<Hierarchy>> leftDims =
            getDimensionHierarchies(leftHierarchies);
        Map<Dimension, Set<Hierarchy>> rightDims =
          getDimensionHierarchies(rightHierarchies);
        for (Dimension dim : leftDims.keySet()) {
            Set<Hierarchy> left = leftDims.get(dim);
            Set<Hierarchy> right = rightDims.get(dim);
            // if different hierarchies from same dimension, members have to be
            // re-fetched with the sql constraints
            if (right != null
                && (left.size() > 1 || right.size() > 1 || !left.equals(right)))
            {
                for (Hierarchy hierLeft : left) {
                    for (Hierarchy hierRight : right) {
                        if (!hierLeft.equals(hierRight)) {
                            RolapHierarchy rhLeft = (RolapHierarchy) hierLeft;
                            RolapHierarchy rhRight = (RolapHierarchy) hierRight;
                            if (checkHierarchiesForExisting(rhLeft, rhRight)) {
                                leftTuples =
                                    RolapNativeExisting
                                        .postFilterExistingRelatedHierarchies(
                                            evaluator,
                                            rhLeft, leftTuples,
                                            rhRight, contextMembers);
                            } else {
                                LOGGER.error(String.format(
                                    "Hierarchy '%s' could not be constrained by same dimension hierarchy '%s'. Only same-table hierarchies are supported by Existing.",
                                    rhLeft.getUniqueName(),
                                    rhRight.getUniqueName()));
                            }
                        }
                    }
                }
            }
        }
        return leftTuples;
    }

    private static boolean checkHierarchiesForExisting(
        RolapHierarchy hierarchy1,
        RolapHierarchy hierarchy2)
    {
        return hierarchy1.getRelation().equals(hierarchy2.getRelation());
    }

    private static Map<Dimension, Set<Hierarchy>> getDimensionHierarchies(
        List<Hierarchy> hierarchies)
    {
        Map<Dimension, Set<Hierarchy>> dims =
            new HashMap<Dimension, Set<Hierarchy>>();
        if (hierarchies != null) {
            for (Hierarchy hierarchy : hierarchies) {
                Set<Hierarchy> list = dims.get(hierarchy.getDimension());
                if (list == null) {
                    list = new HashSet<Hierarchy>();
                    dims.put(hierarchy.getDimension(), list);
                }
                list.add(hierarchy);
            }
        }
        return dims;
    }
}
// End ExistingFunDef.java