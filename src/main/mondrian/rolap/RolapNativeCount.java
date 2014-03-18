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

import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NativeEvaluator;

/**
 * Computes a Count(set) in SQL.
 *
 * @author Will Gorman (wgorman@pentaho.com)
 */
public class RolapNativeCount extends RolapNativeSet {

    public RolapNativeCount() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeCount.get());
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
                evaluator, restrictMemberTypes()))
        {
            return null;
        }
        // is this "Count(<set>, <numeric expr>)"
        String funName = fun.getName();
        if (!"Count".equalsIgnoreCase(funName)) {
            return null;
        }
        
        if (!(args[0] instanceof ResolvedFunCall)) {
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
        
        // TODO: Need to support empty and non-empty second parameter in the correct manner here
        
        NativeEvaluator eval = evaluator.getSchemaReader().getSchema().getNativeRegistry().createEvaluator(
            evaluator, call.getFunDef(), call.getArgs());
        
        if (eval != null) {
            LOGGER.debug("using native count");  
        }
        return eval;
    }
}

// End RolapNativeFilter.java
