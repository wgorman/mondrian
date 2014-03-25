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
import mondrian.calc.HierarchyCalc;
import mondrian.calc.MemberCalc;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Id;
import mondrian.olap.MatchType;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.OlapElement;
import mondrian.olap.SchemaReader;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Definition of the <code>LinkMember</code> MDX function.<br>
 *
 */
public class LinkMemberFunDef extends FunDefBase {

    static final LinkMemberFunDef instance = new LinkMemberFunDef();

    LinkMemberFunDef() {
        super(
            "LinkMember",
            "Returns the member from the specified hierarchy that matches the key values at each level of the specified member in a related hierarchy.",
            "fmmh");
    }

    public Type getResultType(Validator validator, Exp[] args) {
        if (args.length != 2) {
            return null;
        }
        return castType(args[1].getType(), getReturnCategory());
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        // TODO: has no validations

        final MemberCalc memberArg = compiler.compileMember(call.getArg(0));
        final HierarchyCalc hierarchyArg =
            compiler.compileHierarchy(call.getArg(1));
        return new AbstractMemberCalc(
            call,
            new Calc[] {memberArg, hierarchyArg})
        {
          public Member evaluateMember(Evaluator evaluator) {
              RolapMember refMember =
                  (RolapMember) memberArg.evaluateMember(evaluator);
              RolapHierarchy targetHierarchy =
                  (RolapHierarchy) hierarchyArg.evaluateHierarchy(evaluator);
              return getLinkMember(evaluator, refMember, targetHierarchy);
          }
        };
    }

    private static Member getLinkMember(
        Evaluator evaluator,
        RolapMember member,
        RolapHierarchy hierarchy)
    {
        boolean hasParentChild = false;
        // fetch all ancestors to get full key suite
        ArrayList<RolapMember> memberAndAncestors =
            new ArrayList<RolapMember>(member.getDepth() + 1);
        memberAndAncestors.add(member);
        for (
            RolapMember parentMember = member.getParentMember();
            parentMember != null;
            parentMember = parentMember.getParentMember())
        {
            memberAndAncestors.add(parentMember);
            hasParentChild |= parentMember.getLevel().isParentChild();
        }
        return getLinkMember(
            evaluator, hierarchy, memberAndAncestors, hasParentChild);
    }

    private static Member getLinkMember(
        Evaluator evaluator,
        RolapHierarchy target,
        List<RolapMember> referenceMembers,
        boolean hasParentChild)
    {
        // get key values
        ArrayList<Comparable> keys =
            new ArrayList<Comparable>(referenceMembers.size() + 1);
        ArrayList<MondrianDef.Expression> keyColumns =
            new ArrayList<MondrianDef.Expression>();
        for (RolapMember member : referenceMembers) {
            RolapLevel level = member.getLevel();
            if (level.getKeyExp() != null) {
                Object key = member.getKey();
                keys.add(
                    (key instanceof Comparable)
                        ? (Comparable) key
                        : key.toString());
                keyColumns.add(level.getKeyExp());
            }
        }
        // sort keys by descending level and start lookup from top level
        SchemaReader reader = evaluator.getSchemaReader();
        Collections.reverse(keys);
        final int firstLevel = target.hasAll() ? 1 : 0;
        OlapElement current = target.getLevels()[firstLevel];
        for (Object key : keys) {
            Id.KeySegment keySegment =
                new Id.KeySegment(new Id.NameSegment(key.toString()));
            current = current.lookupChild(reader, keySegment, MatchType.EXACT);
        }
        return (current instanceof Member) ? (Member) current : null;
    }

}
// End LinkMemberFunDef.java