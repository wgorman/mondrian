/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2004-2005 TONBELLER AG
 * Copyright (C) 2005-2005 Julian Hyde
 * Copyright (C) 2005-2015 Pentaho and others
 * All Rights Reserved.
 */

package mondrian.rolap;

import java.util.List;

/**
 * A class that is capable of combining multiple requests into single
 * native calls.
 */
public interface NativeRequestConsolidator {

    /**
     * Add a request to this compound request. return true
     * if the request was added, i.e., the implementation can handle
     * the particular request.
     *
     * @param request  the request to add.
     * @return  true if it was added, otherwise false.
     */
    public boolean addRequest(RolapNativeRequest request);

    /**
     * Get the list of requests that have been consolidated. This could
     * return this, if there is only one in the list (i.e. all requests
     * were successfully consolidated), or it could be a number of requests.
     * @return
     */
    public List<RolapNativeRequest> getConsolidatedRequests();

}
