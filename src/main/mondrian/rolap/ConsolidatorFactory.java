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

/**
 * Creates a new ConsolidatedNativeRequest instance.
 * This can be null, for example if consolidation is disabled.
 */
public interface ConsolidatorFactory {

    /**
     * A new request, or an old one that can safely be reused.
     * @return the request.
     */
    public NativeRequestConsolidator newConsolidator();

}
