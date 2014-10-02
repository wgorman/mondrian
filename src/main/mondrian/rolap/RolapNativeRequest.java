/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2014 Pentaho
// All Rights Reserved.
//
*/
package mondrian.rolap;

/**
 * A <code>RolapNativeRequest</code> contains a callback to execute a specific
 * Native evaluation request.
 */
public interface RolapNativeRequest {
    public void execute();
}

// End RolapNativeRequest.java
