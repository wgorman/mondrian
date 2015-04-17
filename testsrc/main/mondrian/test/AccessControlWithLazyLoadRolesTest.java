/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2015 Pentaho Corporation.  All rights reserved.
*/

package mondrian.test;

import mondrian.olap.MondrianProperties;

/**
 * Subclass of {@link AccessControlTest} that runs
 * with {@link mondrian.olap.MondrianProperties#LazyLoadRoles}=true.
 */
public class AccessControlWithLazyLoadRolesTest extends AccessControlTest
{
    /**
     * Creates a AccessControlWithLazyLoadRolesTest.
     *
     * @param name Testcase name
     */
    public AccessControlWithLazyLoadRolesTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        propSaver.set(
            MondrianProperties.instance().LazyLoadRoles,
            true);
    }
}
