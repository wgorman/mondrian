/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.NullType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;

import java.util.List;

/**
 * Definition of the <code>StrToMember</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>StrToMember(&lt;String Expression&gt;)
 * </code></blockquote>
 */
class StrToMemberFunDef extends FunDefBase {
    static final ResolverImpl Resolver = new ResolverImpl();

    private StrToMemberFunDef() {
        super(
            "StrToMember",
            "Returns a member from a unique name String in MDX format.",
            "fmS");
    }

    private StrToMemberFunDef(int[] parameterTypes) {
        super(
                "StrToMember",
                "<Member> StrToMember(<String Expression>[, CONSTRAINED])",
                "Returns a member from a unique name String in MDX format.",
                Syntax.Function, Category.Member, parameterTypes);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final StringCalc memberNameCalc =
            compiler.compileString(call.getArg(0));
        return new AbstractMemberCalc(call, new Calc[] {memberNameCalc}) {
            public Member evaluateMember(Evaluator evaluator) {
                String memberName =
                    memberNameCalc.evaluateString(evaluator);
                if (memberName == null) {
                    throw newEvalException(
                        MondrianResource.instance().NullValue.ex());
                }
                return parseMember(evaluator, memberName, null);
            }
        };
    }

    private static class ResolverImpl extends ResolverBase {
        private static final String[] KEYWORDS = new String[] { "CONSTRAINED" };

        ResolverImpl() {
            super(
                    "StrToMember",
                    "<Member> StrToMember(<String Expression>[, CONSTRAINED])",
                    "Returns a member from a unique name String in MDX format.",
                    Syntax.Function);
        }

        public String[] getReservedWords() {
            return KEYWORDS;
        }

        public FunDef resolve(
                Exp[] args,
                Validator validator,
                List<Conversion> conversions)
        {
            if (args.length < 1) {
                return null;
            }

            Type type = args[0].getType();
            if (!(type instanceof StringType) && !(type instanceof NullType))
            {
                return null;
            }

            int[] argTypes = new int[args.length];
            argTypes[0] = Category.String;
            if (args.length == 2 &&
                    validator.canConvert(1, args[1], Category.Symbol, conversions))
            {
                // we don't support CONSTRAINED syntax, but we'll allow it
                argTypes[1] = Category.Symbol;
            }

            return new StrToMemberFunDef(argTypes);
        }

        public FunDef getFunDef() {
            return new StrToMemberFunDef(new int[] {Category.String});
        }
    }
}

// End StrToMemberFunDef.java
