/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.util.Pair;

/**
 * The 'All' member of a {@link mondrian.rolap.RolapCubeHierarchy}.
 *
 * <p>A minor extension to {@link mondrian.rolap.RolapCubeMember} because the
 * naming rules are different.
 *
 * @author Will Gorman, 19 October 2007
 */
class RolapAllCubeMember
    extends RolapCubeMember
{
    protected final String name;
    private final String uniqueName;

    /**
     * Creates a RolapAllCubeMember.
     *
     * @param member Member of underlying (non-cube) hierarchy
     * @param cubeLevel Level
     */
    public RolapAllCubeMember(RolapMember member, RolapCubeLevel cubeLevel)
    {
        super(null, member, cubeLevel);
        assert member.isAll();

        Pair<String, String> pair =
            foo(member.getName(), cubeLevel.cubeHierarchy);
        this.name = pair.right;
        this.uniqueName = pair.left;
    }

    static Pair<String, String> foo(
        String memberName,
        RolapCubeHierarchy hierarchy)
    {
        String name;

        // replace hierarchy name portion of all member with new name
        if (hierarchy.getName().equals(
                hierarchy.getRolapHierarchy().getName()))
        {
            name = memberName;
        } else {
            // special case if we're dealing with a closure
            String replacement =
                hierarchy.getName().replaceAll("\\$", "\\\\\\$");

            // convert string to regular expression
            String memberLevelName =
                hierarchy.getName().replaceAll("\\.", "\\\\.");

            name = memberName.replaceAll(memberLevelName, replacement);
        }

        // Assign unique name. We use a kludge to ensure that calc members are
        // called [Measures].[Foo] not [Measures].[Measures].[Foo]. We can
        // remove this code when we revisit the scheme to generate member unique
        // names.
        String uniqueName = Util.makeFqName(hierarchy, name);
        return Pair.of(name, uniqueName);
    }

    public String getName() {
        return name;
    }

    @Override
    public String getUniqueName() {
        return uniqueName;
    }
}

// End RolapAllCubeMember.java
