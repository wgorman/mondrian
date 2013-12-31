/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap;

import java.util.ArrayList;
import java.util.List;
import mondrian.olap.MondrianDef;

/**
 * Provides the base cubes that a virtual cube uses and
 * specifies if unrelated dimensions to measures from these cubes should be
 * ignored.
 *
 * @author ajoglekar
 * @since Nov 22 2007
 */
public class RolapCubeUsages {
    private MondrianDef.CubeUsages cubeUsages;
    private List<String> cubeNames;
    public RolapCubeUsages(MondrianDef.CubeUsages cubeUsage) {
        this.cubeUsages = cubeUsage;
        this.cubeNames = new ArrayList<String>();
        if (cubeUsages != null && cubeUsages.cubeUsages != null) {
          for (MondrianDef.CubeUsage usage : cubeUsages.cubeUsages) {
            cubeNames.add( usage.cubeName );
          }
        }
    }
    
    public List<String> getCubeNames() {
      return cubeNames;
    }

    public boolean shouldIgnoreUnrelatedDimensions(String baseCubeName) {
        if (cubeUsages == null || cubeUsages.cubeUsages == null) {
            return false;
        }
        for (MondrianDef.CubeUsage usage : cubeUsages.cubeUsages) {
            if (usage.cubeName.equals(baseCubeName)
                && Boolean.TRUE.equals(usage.ignoreUnrelatedDimensions))
            {
                return true;
            }
        }
        return false;
    }
}

// End RolapCubeUsages.java
