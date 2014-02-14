package mondrian.rolap;

import java.util.Arrays;

import mondrian.olap.Id;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;

/**
 * Constrains children request by key, for providing ssas-like key access.
 */
class ChildByKeyConstraint extends DefaultMemberChildrenConstraint {

    private final String childKey;
    private final Object cacheKey;

    public ChildByKeyConstraint(Id.KeySegment childKey) {
        // assuming only one key segment, discarding the rest
        this.childKey = childKey.getKeyParts().get(0).name;
        this.cacheKey = Arrays.asList(ChildByKeyConstraint.class, childKey);
    }

    public void addLevelConstraint(
        SqlQuery query,
        RolapCube baseCube,
        AggStar aggStar,
        RolapLevel level)
    {
        super.addLevelConstraint(query, baseCube, aggStar, level);
        query.addWhere(SqlConstraintUtils.constrainLevel2(
            query, level.getKeyExp(), level.getDatatype(), childKey) );
    }

    public boolean equals(Object obj) {
        return obj instanceof ChildByKeyConstraint &&
            getCacheKey().equals(((ChildByKeyConstraint)obj).getCacheKey());
    }

    public int hashCode() {
        return getCacheKey().hashCode();
    }

    public String toString() {
        return "ChildByNameConstraint(" + childKey + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }
}
