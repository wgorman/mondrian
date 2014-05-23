/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2014 Pentaho
// All rights reserved.
*/
package mondrian.rolap;

import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Util;
import mondrian.rolap.sql.CrossJoinArg;

import java.util.List;

public class RolapNativeExisting extends RolapNativeSet {

    public RolapNativeExisting() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeExisting.get());
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
        // attempt to nativize the set expression
        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

        if (failedCjArg(allArgs)) {
            alertNonNative(evaluator, fun, args[0]);
            return null;
        }
        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            return null;
        }

        CrossJoinArg[] predicateArgs = allArgs.size() > 1
            ? allArgs.get(1)
            : null;
        CrossJoinArg[] combinedArgs = predicateArgs != null
            ? Util.appendArrays(cjArgs, predicateArgs)
            : cjArgs;

        ExistingConstraint constraint = new ExistingConstraint(
            combinedArgs,
            evaluator);
        LOGGER.debug("native EXISTING");
        return new SetEvaluator(
            cjArgs,
            evaluator.getSchemaReader(),
            constraint);
    }

    protected boolean isJoinRequired() {
        return false;
    }

    private static class ExistingConstraint extends SetConstraint {
        // only need the context
        ExistingConstraint(CrossJoinArg[] args, RolapEvaluator evaluator) {
            super(args, evaluator, true);
        }
    }

}
// End RolapNativeExisting.java