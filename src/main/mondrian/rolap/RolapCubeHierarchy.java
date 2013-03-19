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
import mondrian.olap.Member.MemberType;
import mondrian.olap.fun.VisualTotalsFunDef;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.util.*;

import java.sql.SQLException;
import java.util.*;

/**
 * Hierarchy that is associated with a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeHierarchy extends RolapHierarchy {

    private final RolapCubeDimension cubeDimension;
    private final RolapHierarchy rolapHierarchy;
    private final int ordinal;

    /**
     * Length of prefix to be removed when translating member unique names, or
     * 0 if no translation is necessary.
     */
    private int removePrefixLength;

    /**
     * The raw member reader. For a member reader which incorporates access
     * control and deals with hidden members (if the hierarchy is ragged), use
     * {@link #createMemberReader(Role)}.
     */
    private MemberReader memberReader;

    RolapMember defaultMember;

    /**
     * Creates a RolapCubeHierarchy.
     *
     * @param schemaLoader Schema loader
     * @param cubeDimension Dimension
     * @param rolapHierarchy Wrapped hierarchy
     * @param subName Name of hierarchy within dimension
     * @param uniqueName Unique name of hierarchy
     * @param ordinal Ordinal of hierarchy within cube
     */
    public RolapCubeHierarchy(
        RolapSchemaLoader schemaLoader,
        RolapCubeDimension cubeDimension,
        RolapHierarchy rolapHierarchy,
        String subName,
        String uniqueName,
        int ordinal,
        Larder larder)
    {
        super(
            cubeDimension,
            subName,
            uniqueName,
            rolapHierarchy.isVisible(),
            rolapHierarchy.hasAll(),
            null,
            rolapHierarchy.attribute,
            larder);
        this.ordinal = ordinal;
        this.rolapHierarchy = rolapHierarchy;
        this.cubeDimension = cubeDimension;
    }

    void initCubeHierarchy(
        RolapSchemaLoader schemaLoader,
        String memberReaderClass,
        String allMemberName,
        String allMemberCaption)
    {
        // Compute whether the unique names of members of this hierarchy are
        // different from members of the underlying hierarchy. If so, compute
        // the length of the prefix to be removed before this hierarchy's unique
        // name is added. For example, if this.uniqueName is "[Ship Time]" and
        // rolapHierarchy.uniqueName is "[Time]", remove prefixLength will be
        // length("[Ship Time]") = 11.
        if (uniqueName.equals(rolapHierarchy.getUniqueName())) {
            this.removePrefixLength = 0;
        } else {
            this.removePrefixLength = rolapHierarchy.getUniqueName().length();
        }

        for (RolapLevel level : rolapHierarchy.getLevelList()) {
            final Map<String, List<Larders.Resource>> resourceMap;
            final BitSet bitSet =
                schemaLoader.resourceHierarchyTags.get(
                    getCube() + "." + uniqueName);
            if (bitSet != null
                && (bitSet.get(level.getDepth())
                    || level.isAll()))
            {
                // We can't be sure whether there is a resource for the 'all'
                // member because we don't know its name when we are parsing
                // the resource file, so always give the 'all' level a resource
                // map.
                resourceMap = schemaLoader.resourceMap;
            } else {
                resourceMap = null;
            }
            levelList.add(new RolapCubeLevel(level, this, resourceMap));
        }

        if (hasAll) {
            allLevel = getLevelList().get(0);
        } else {
            // create an all level if one doesn't normally
            // exist in the hierarchy
            allLevel =
                new RolapCubeLevel(
                    rolapHierarchy.allLevel,
                    this,
                    schemaLoader.resourceMap);
            allLevel.initLevel(schemaLoader, false);
        }

        // Create an all member.
        final String name = Util.first(allMemberName, "All " + this.name + "s");
        Pair<String, String> pair =
            RolapAllCubeMember.foo(
                name,
                this);
        if (false)
        this.allMember =
            new RolapMemberBase(
                null,
                (RolapCubeLevel) allLevel,
                null,
                MemberType.ALL,
                pair.right,
                getLarder());

        final Larders.LarderBuilder builder = new Larders.LarderBuilder();
        builder.name(name);
        if (allMemberCaption != null && !allMemberCaption.equals(name)) {
            builder.caption(allMemberCaption);
        }
        this.allMember =
            new RolapMemberBase(
                null,
                (RolapCubeLevel) allLevel,
                Util.COMPARABLE_EMPTY_LIST,
                Member.MemberType.ALL,
                Util.makeFqName(allLevel.getHierarchy(), name),
                builder.build());
        this.allMember.setOrdinal(0);

        this.nullLevel =
            new RolapCubeLevel(rolapHierarchy.nullLevel, this, null);
        this.nullMember = new RolapNullMember((RolapCubeLevel) nullLevel);
    }

    @Override
    void init2(RolapSchemaLoader schemaLoader) {
        super.init2(schemaLoader);
        if (memberReader == null) {
            memberReader =
                getRolapSchema().createMemberReader(this, null);
        }
    }

    // override with stricter return type
    @Override
    public RolapCubeDimension getDimension() {
        return (RolapCubeDimension) dimension;
    }

    public RolapHierarchy getRolapHierarchy() {
        return rolapHierarchy;
    }

    public final int getOrdinalInCube() {
        return ordinal;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RolapCubeHierarchy)) {
            return false;
        }

        RolapCubeHierarchy that = (RolapCubeHierarchy)o;
        return cubeDimension.equals(that.cubeDimension)
            && getUniqueName().equals(that.getUniqueName());
    }

    protected int computeHashCode() {
        return Util.hash(super.computeHashCode(), this.cubeDimension.cube);
    }

    public Member createMember(
        Member parent,
        Level level,
        String name,
        Formula formula)
    {
        final RolapCubeLevel cubeLevel = (RolapCubeLevel) level;
        final RolapMember cubeParent = (RolapMember) parent;
        if (formula == null) {
            return new RolapMemberBase(
                cubeParent, cubeLevel, name, MemberType.REGULAR,
                RolapMemberBase.deriveUniqueName(
                    cubeParent, cubeLevel, name, false),
                Larders.ofName(name));
        } else if (level.getDimension().isMeasures()) {
            return new RolapCalculatedMeasure(
                cubeParent, cubeLevel, name, formula);
        } else {
            return new RolapCalculatedMember(
                cubeParent, cubeLevel, name, formula);
        }
    }

    public final RolapMember getDefaultMember() {
        assert defaultMember != null;
        return defaultMember;
    }

    /**
     * Sets default member of this Hierarchy.
     *
     * @param member Default member
     */
    public void setDefaultMember(RolapMember member) {
        if (member != null) {
            this.defaultMember = member;
        }
    }

    void setMemberReader(MemberReader memberReader) {
        this.memberReader = memberReader;
    }

    final MemberReader getMemberReader() {
        return memberReader;
    }

    /**
     * Creates a member reader which enforces the access-control profile of
     * <code>role</code>.
     *
     * <p>This method may not be efficient, so the caller should take care
     * not to call it too often. A cache is a good idea.
     *
     * @param role Role (not null)
     * @return Member reader that implements access control (never null)
     */
    MemberReader createMemberReader(Role role) {
        return createMemberReader(this, role);
    }

    /**
     * Converts the unique name of a member of the underlying hierarchy to
     * the appropriate name for this hierarchy.
     *
     * <p>For example, if the shared hierarchy is [Time].[Quarterly] and the
     * hierarchy usage is [Ship Time].[Quarterly], then [Time].[1997].[Q1] would
     * be translated to [Ship Time].[Quarterly].[1997].[Q1].
     *
     * @param memberUniqueName Unique name of member from underlying hierarchy
     * @return Translated unique name
     */
    final String convertMemberName(String memberUniqueName) {
        if (removePrefixLength > 0
            && !memberUniqueName.startsWith(uniqueName))
        {
            return uniqueName + memberUniqueName.substring(removePrefixLength);
        }
        return memberUniqueName;
    }

    public final RolapCube getCube() {
        return cubeDimension.cube;
    }

    private static RolapCubeMember createAncestorMembers(
        RolapCubeHierarchyMemberReader memberReader,
        RolapCubeLevel level,
        RolapMember member)
    {
        if (member == null) {
            return null;
        }
        RolapCubeMember parent = null;
        if (member.getParentMember() != null) {
            parent =
                createAncestorMembers(
                    memberReader,
                    level.getParentLevel(),
                    member.getParentMember());
        }
        return memberReader.lookupCubeMember(parent, member, level);
    }

    public List<? extends RolapCubeLevel> getLevelList() {
        return Util.cast(levelList);
    }

    /**
     * Extension to {@link MemberReader} that caches {@link RolapCubeMember}
     * objects.
     *
     * <p>TODO: Since this is part of a caching strategy, should be implemented
     * as a Strategy Pattern, avoiding hierarchy.</p>
     *
     * <h3>RolapCubeMember Caching Approach</h3>
     *
     * <dl>
     *
     * <dt>RolapHierarchy.SmartMemberReader.SmartCacheHelper</dt>
     *
     * <dd>This is the shared cache across shared hierarchies.  This member
     * cache only contains members loaded by non-cube specific member lookups.
     * This cache should only contain RolapMembers, not RolapCubeMembers.</dd>
     *
     * <dt>{@link mondrian.rolap.RolapCubeHierarchy.RolapCubeHierarchyMemberReader#getRolapCubeMemberCacheHelper()}</dt>
     *
     * <dd>This cache contains the RolapCubeMember objects, which are
     * cube specific wrappers of shared members.</dd>
     *
     * <dt>{@link RolapCubeHierarchy.RolapCubeHierarchyMemberReader}.SmartCacheHelper</dt>
     *
     * <dd>This is the inherited shared cache from SmartMemberReader, and is
     * used when a join with the fact table is necessary, aka a
     * SqlContextConstraint is used. This cache may be redundant with
     * rolapCubeCacheHelper.</dd>
     *
     * </dl>
     *
     * <p>A Special note regarding
     * {@link CacheRolapCubeHierarchyMemberReader#cubeSource}.
     * This class was required for the special situation getMemberBuilder()
     * method call from RolapNativeSet.  This class utilizes both the
     * rolapCubeCacheHelper class for storing RolapCubeMembers, and also the
     * RolapCubeHierarchyMemberReader's inherited SmartCacheHelper.</p>
     */
    public static interface RolapCubeHierarchyMemberReader
        extends MemberReader
    {
        public RolapCubeMember lookupCubeMember(
            final RolapCubeMember parent,
            final RolapMember member,
            final RolapCubeLevel level);

        public MemberCacheHelper getRolapCubeMemberCacheHelper();
    }


    /**
     *  member reader wrapper - uses existing member reader,
     *  but wraps and caches all intermediate members.
     *
     *  <p>Synchronization. Most synchronization takes place within
     * SmartMemberReader.  All synchronization is done on the cacheHelper
     * object.
      */
    public class CacheRolapCubeHierarchyMemberReader
        extends SmartMemberReader
        implements RolapCubeHierarchyMemberReader
    {
        /**
         * cubeSource is passed as our member builder
         */
        protected final RolapCubeSqlMemberSource cubeSource;

        /**
         * this cache caches RolapCubeMembers that are light wrappers around
         * shared and non-shared Hierarchy RolapMembers.  The inherited
         * cacheHelper object contains non-shared hierarchy RolapMembers.
         * non-shared hierarchy RolapMembers are created when a member lookup
         * involves the Cube's fact table.
         */
        protected MemberCacheHelper rolapCubeCacheHelper;

        public CacheRolapCubeHierarchyMemberReader() {
            super(new SqlMemberSource(RolapCubeHierarchy.this));
            rolapCubeCacheHelper =
                new MemberCacheHelper(RolapCubeHierarchy.this);

            cubeSource =
                new RolapCubeSqlMemberSource(
                    this,
                    RolapCubeHierarchy.this,
                    rolapCubeCacheHelper,
                    cacheHelper);

            cubeSource.setCache(getMemberCache());
        }

        public MemberBuilder getMemberBuilder() {
            return this.cubeSource;
        }

        public MemberCacheHelper getRolapCubeMemberCacheHelper() {
            return rolapCubeCacheHelper;
        }

        public List<RolapMember> getRootMembers() {
            if (rootMembers == null) {
                rootMembers = getMembersInLevel(getLevelList().get(0));
            }
            return rootMembers;
        }

        protected void readMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            List<RolapMember> rolapChildren = new ArrayList<RolapMember>();
            List<RolapMember> rolapParents = new ArrayList<RolapMember>();
            Map<String, RolapCubeMember> lookup =
                new HashMap<String, RolapCubeMember>();

            // extract RolapMembers from their RolapCubeMember objects
            // populate lookup for reconnecting parents and children
            for (RolapMember member : parentMembers) {
                if (member instanceof VisualTotalsFunDef.VisualTotalMember) {
                    continue;
                }
                final RolapCubeMember cubeMember = (RolapCubeMember) member;
                final RolapMember rolapMember = cubeMember.getRolapMember();
                lookup.put(rolapMember.getUniqueName(), cubeMember);
                rolapParents.add(rolapMember);
            }

            // get member children from shared member reader if possible,
            // if not get them from our own source
            boolean joinReq =
                (constraint instanceof SqlContextConstraint);
            if (joinReq) {
                super.readMemberChildren(
                    parentMembers, rolapChildren, constraint);
            } else {
                memberReader.getMemberChildren(
                    rolapParents, rolapChildren, constraint);
            }

            // now lookup or create RolapCubeMember
            for (RolapMember currMember : rolapChildren) {
                RolapCubeMember parent =
                    lookup.get(
                        currMember.getParentMember().getUniqueName());
                RolapCubeLevel level =
                    parent.getLevel().getChildLevel();
                if (level == null) {
                    // most likely a parent child hierarchy
                    level = parent.getLevel();
                }
                RolapCubeMember newmember =
                    lookupCubeMember(
                        parent, currMember, level);
                children.add(newmember);
            }

            // Put them in a temporary hash table first. Register them later,
            // when we know their size (hence their 'cost' to the cache pool).
            Map<RolapMember, List<RolapMember>> tempMap =
                new HashMap<RolapMember, List<RolapMember>>();
            for (RolapMember member1 : parentMembers) {
                tempMap.put(member1, Collections.<RolapMember>emptyList());
            }

            // note that this stores RolapCubeMembers in our cache,
            // which also stores RolapMembers.

            for (RolapMember child : children) {
            // todo: We could optimize here. If members.length is small, it's
            // more efficient to drive from members, rather than hashing
            // children.length times. We could also exploit the fact that the
            // result is sorted by ordinal and therefore, unless the "members"
            // contains members from different levels, children of the same
            // member will be contiguous.
                assert child != null : "child";
                final RolapMember parentMember = child.getParentMember();
                List<RolapMember> cacheList = tempMap.get(parentMember);
                if (cacheList == null) {
                    // The list is null if, due to dropped constraints, we now
                    // have a children list of a member we didn't explicitly
                    // ask for it. Adding it to the cache would be viable, but
                    // let's ignore it.
                    continue;
                } else if (cacheList == Collections.EMPTY_LIST) {
                    cacheList = new ArrayList<RolapMember>();
                    tempMap.put(parentMember, cacheList);
                }
                cacheList.add(child);
            }

            synchronized (cacheHelper) {
                for (Map.Entry<RolapMember, List<RolapMember>> entry
                    : tempMap.entrySet())
                {
                    final RolapMember member = entry.getKey();
                    if (rolapCubeCacheHelper.getChildrenFromCache(
                            member, constraint) == null)
                    {
                        final List<RolapMember> cacheList = entry.getValue();
                        if (false) {
                            rolapCubeCacheHelper.putChildren(
                                member, constraint, cacheList);
                        }
                    }
                }
            }
        }

        public void getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            if (RolapCubeHierarchy.this.getDimension().isMeasures()) {
                // Members of measures dimensions are not RolapCubeMembers.
                memberReader.getMemberChildren(
                    parentMembers, children, constraint);
                return;
            }
            synchronized (cacheHelper) {
                List<RolapMember> missed = new ArrayList<RolapMember>();
                for (RolapMember parentMember : parentMembers) {
                    List<RolapMember> list =
                        rolapCubeCacheHelper.getChildrenFromCache(
                            parentMember, constraint);
                    if (list == null) {
                        // the null member has no children
                        if (!parentMember.isNull()) {
                            missed.add(parentMember);
                        }
                    } else {
                        children.addAll(list);
                    }
                }
                if (missed.size() > 0) {
                    readMemberChildren(missed, children, constraint);
                }
            }
        }


        public List<RolapMember> getMembersInLevel(
            RolapCubeLevel level,
            TupleConstraint constraint)
        {
            synchronized (cacheHelper) {
                List<RolapMember> members =
                    rolapCubeCacheHelper.getLevelMembersFromCache(
                        level, constraint);
                if (members != null) {
                    return members;
                }

                // if a join is required, we need to pass in the RolapCubeLevel
                // vs. the regular level
                boolean joinReq =
                    (constraint instanceof SqlContextConstraint);
                List<RolapMember> list;
                if (!joinReq) {
                    list = memberReader.getMembersInLevel(level, constraint);
                } else {
                    list =
                        super.getMembersInLevel(
                            level, constraint);
                }
                List<RolapMember> newlist = new ArrayList<RolapMember>();
                for (RolapMember member : list) {
                    // note that there is a special case for the all member

                    // REVIEW: disabled, to see what happens. if this code is
                    // for performance, we should check level.isAll at the top
                    // of the method; if it is for correctness, leave the code
                    // in
                    if (false && member == rolapHierarchy.getAllMember()) {
                        newlist.add(getAllMember());
                    } else {
                        RolapCubeMember cubeMember =
                            lookupCubeMemberWithParent(
                                member,
                                level);
                        newlist.add(cubeMember);
                    }
                }
                rolapCubeCacheHelper.putLevelMembersInCache(
                    level, constraint, newlist);

                return newlist;
            }
        }

        private RolapCubeMember lookupCubeMemberWithParent(
            RolapMember member,
            RolapCubeLevel cubeLevel)
        {
            final RolapMember parentMember = member.getParentMember();
            final RolapCubeMember parentCubeMember;
            if (parentMember == null) {
                parentCubeMember = null;
            } else {
                // In parent-child hierarchies, a member's parent may be in the
                // same level.
                final RolapCubeLevel parentLevel =
                    parentMember.getLevel() == member.getLevel()
                        ? cubeLevel
                        : cubeLevel.getParentLevel();
                parentCubeMember =
                    lookupCubeMemberWithParent(
                        parentMember, parentLevel);
            }
            return lookupCubeMember(
                parentCubeMember, member, cubeLevel);
        }

        @Override
        public RolapMember getMemberByKey(
            RolapCubeLevel level, List<Comparable> keyValues)
        {
            synchronized (cacheHelper) {
                final RolapMember member =
                    super.getMemberByKey(level, keyValues);
                return createAncestorMembers(this, level, member);
            }
        }

        public RolapCubeMember lookupCubeMember(
            RolapCubeMember parent,
            RolapMember member,
            RolapCubeLevel level)
        {
            synchronized (cacheHelper) {
                if (member.getKey() == Util.COMPARABLE_EMPTY_LIST) {
                    if (member.isAll()) {
                        return (RolapCubeMember) getAllMember();
                    }

                    throw new RuntimeException(member.toString());
                }

                RolapCubeMember cubeMember;
                if (false) {
                    Object key = member.getKeyAsList();
                    cubeMember = (RolapCubeMember)
                        rolapCubeCacheHelper.getMember(level, key);
                    if (cubeMember == null) {
                        cubeMember = null; // wrapMember(parent, member, level);
                        rolapCubeCacheHelper.putMember(level, key, cubeMember);
                    }
                } else {
                    cubeMember = null; // wrapMember(parent, member, level);
                }
                return cubeMember;
            }
        }

        public int getMemberCount() {
            return memberReader.getMemberCount();
        }
    }

    /**
     * Same as {@link RolapCubeHierarchyMemberReader} but without caching
     * anything.
     */
    public class NoCacheRolapCubeHierarchyMemberReader
        extends NoCacheMemberReader
        implements RolapCubeHierarchyMemberReader
    {
        /**
         * cubeSource is passed as our member builder
         */
        protected final RolapCubeSqlMemberSource cubeSource;

        /**
         * this cache caches RolapCubeMembers that are light wrappers around
         * shared and non-shared Hierarchy RolapMembers.  The inherited
         * cacheHelper object contains non-shared hierarchy RolapMembers.
         * non-shared hierarchy RolapMembers are created when a member lookup
         * involves the Cube's fact table.
         */
        protected MemberCacheHelper rolapCubeCacheHelper;

        public NoCacheRolapCubeHierarchyMemberReader() {
            super(new SqlMemberSource(RolapCubeHierarchy.this));
            rolapCubeCacheHelper =
                new MemberNoCacheHelper();

            cubeSource =
                new RolapCubeSqlMemberSource(
                    this,
                    RolapCubeHierarchy.this,
                    rolapCubeCacheHelper,
                    new MemberNoCacheHelper());

            cubeSource.setCache(rolapCubeCacheHelper);
        }

        public MemberBuilder getMemberBuilder() {
            return this.cubeSource;
        }

        public MemberCacheHelper getRolapCubeMemberCacheHelper() {
            return rolapCubeCacheHelper;
        }

        public List<RolapMember> getRootMembers() {
            return getMembersInLevel(getLevelList().get(0));
        }

        protected void readMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            List<RolapMember> rolapChildren = new ArrayList<RolapMember>();
            List<RolapMember> rolapParents = new ArrayList<RolapMember>();
            Map<String, RolapCubeMember> lookup =
                new HashMap<String, RolapCubeMember>();

            // extract RolapMembers from their RolapCubeMember objects
            // populate lookup for reconnecting parents and children
            final List<RolapCubeMember> parentRolapCubeMemberList =
                Util.cast(parentMembers);
            for (RolapCubeMember member : parentRolapCubeMemberList) {
                final RolapMember rolapMember = member.getRolapMember();
                lookup.put(rolapMember.getUniqueName(), member);
                rolapParents.add(rolapMember);
            }

            // get member children from shared member reader if possible,
            // if not get them from our own source
            boolean joinReq =
                (constraint instanceof SqlContextConstraint);
            if (joinReq) {
                super.readMemberChildren(
                    parentMembers, rolapChildren, constraint);
            } else {
                memberReader.getMemberChildren(
                    rolapParents, rolapChildren, constraint);
            }

            // now lookup or create RolapCubeMember
            for (RolapMember currMember : rolapChildren) {
                RolapCubeMember parent =
                    lookup.get(
                        currMember.getParentMember().getUniqueName());
                RolapCubeLevel level =
                    parent.getLevel().getChildLevel();
                if (level == null) {
                    // most likely a parent child hierarchy
                    level = parent.getLevel();
                }
                RolapCubeMember newmember =
                    lookupCubeMember(
                        parent, currMember, level);
                children.add(newmember);
            }

            // Put them in a temporary hash table first. Register them later,
            // when we know their size (hence their 'cost' to the cache pool).
            Map<RolapMember, List<RolapMember>> tempMap =
                new HashMap<RolapMember, List<RolapMember>>();
            for (RolapMember member1 : parentMembers) {
                tempMap.put(member1, Collections.<RolapMember>emptyList());
            }

            // note that this stores RolapCubeMembers in our cache,
            // which also stores RolapMembers.

            for (RolapMember child : children) {
            // todo: We could optimize here. If members.length is small, it's
            // more efficient to drive from members, rather than hashing
            // children.length times. We could also exploit the fact that the
            // result is sorted by ordinal and therefore, unless the "members"
            // contains members from different levels, children of the same
            // member will be contiguous.
                assert child != null : "child";
                final RolapMember parentMember = child.getParentMember();
            }
        }

        public void getMemberChildren(
            List<RolapMember> parentMembers,
            List<RolapMember> children,
            MemberChildrenConstraint constraint)
        {
            List<RolapMember> missed = new ArrayList<RolapMember>();
            for (RolapMember parentMember : parentMembers) {
                // the null member has no children
                if (!parentMember.isNull()) {
                    missed.add(parentMember);
                }
            }
            if (missed.size() > 0) {
                readMemberChildren(missed, children, constraint);
            }
        }


        public List<RolapMember> getMembersInLevel(
            final RolapCubeLevel level,
            TupleConstraint constraint)
        {
            // if a join is required, we need to pass in the RolapCubeLevel
            // vs. the regular level
            boolean joinReq =
                (constraint instanceof SqlContextConstraint);
            final List<RolapMember> list;

            if (!joinReq) {
                list = memberReader.getMembersInLevel(level, constraint);
            } else {
                list =
                    super.getMembersInLevel(
                        level, constraint);
            }

            return new UnsupportedList<RolapMember>() {
                public RolapMember get(final int index) {
                    return mutate(list.get(index));
                }

                public int size() {
                    return list.size();
                }

                public Iterator<RolapMember> iterator() {
                    final Iterator<RolapMember> it = list.iterator();
                    return new Iterator<RolapMember>() {
                        public boolean hasNext() {
                            return it.hasNext();
                        }
                        public RolapMember next() {
                            return mutate(it.next());
                        }

                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                private RolapMember mutate(final RolapMember member) {
                    RolapCubeMember parent = null;
                    if (member.getParentMember() != null) {
                        parent =
                            createAncestorMembers(
                                NoCacheRolapCubeHierarchyMemberReader.this,
                                (RolapCubeLevel) level.getParentLevel(),
                                member.getParentMember());
                    }
                    return lookupCubeMember(
                        parent, member, (RolapCubeLevel) level);
                }
            };
        }

        public RolapCubeMember lookupCubeMember(
            RolapCubeMember parent,
            RolapMember member,
            RolapCubeLevel level)
        {
            if (member.getKey() == Util.COMPARABLE_EMPTY_LIST) {
                if (member.isAll()) {
                    return (RolapCubeMember) getAllMember();
                }

                throw new RuntimeException(member.toString());
            }

            if (member instanceof RolapCubeMember) {
                return (RolapCubeMember) member;
            }
            if (member instanceof RolapStoredMeasure) {
                RolapStoredMeasure storedMeasure = (RolapStoredMeasure) member;
                return new RolapCubeStoredMeasure(parent, storedMeasure, level);
            }
            return new RolapCubeMember(parent, member, level);
        }

        public int getMemberCount() {
            return memberReader.getMemberCount();
        }
    }

    // hack to make the code compile
    private static class MemberNoCacheHelper extends MemberCacheHelper {
        public MemberNoCacheHelper() {
            super(null);
            Util.deprecated("remove", false);
        }
    }

    /**
     * Implementation of {@link mondrian.rolap.SqlMemberSource} and
     * {@link mondrian.rolap.MemberReader} that wraps members from an underlying
     * member reader in a {@link mondrian.rolap.RolapCubeMember}.
     */
    public static class RolapCubeSqlMemberSource extends SqlMemberSource {

        private final RolapCubeHierarchyMemberReader memberReader;
        private final MemberCacheHelper memberSourceCacheHelper;
        private final Object memberCacheLock;

        /**
         * Creates a RolapCubeSqlMemberSource.
         *
         * @param memberReader Underlying member reader that creates shared
         *   members. These members implement {@link mondrian.rolap.RolapMember}
         *   but not {@link mondrian.rolap.RolapCubeMember}.
         * @param hierarchy Hierarchy
         * @param memberSourceCacheHelper Helper to put members into cache
         * @param memberCacheLock Lock for cache modifications
         */
        public RolapCubeSqlMemberSource(
            RolapCubeHierarchyMemberReader memberReader,
            RolapCubeHierarchy hierarchy,
            MemberCacheHelper memberSourceCacheHelper,
            Object memberCacheLock)
        {
            super(hierarchy);
            this.memberReader = memberReader;
            this.memberSourceCacheHelper = memberSourceCacheHelper;
            this.memberCacheLock = memberCacheLock;
        }

        public RolapMember makeMember(
            RolapMember parentMember,
            RolapCubeLevel childLevel,
            Comparable key,
            Object captionValue,
            String nameValue,
            Comparable orderKey,
            boolean parentChild,
            SqlStatement stmt,
            SqlTupleReader.LevelColumnLayout layout)
            throws SQLException
        {
            final RolapCubeMember parentCubeMember =
                (RolapCubeMember) parentMember;
            final RolapMember parent;
            if (parentMember != null) {
                parent = parentCubeMember.getRolapMember();
            } else {
                parent = null;
            }
            RolapMember member =
                super.makeMember(
                    parent,
                    childLevel,
                    key,
                    captionValue,
                    nameValue,
                    orderKey,
                    parentChild,
                    stmt,
                    layout);
            return memberReader.lookupCubeMember(
                parentCubeMember, member, childLevel);
        }

        public MemberCache getMemberCache() {
            // this is a special cache used solely for rolapcubemembers
            return memberSourceCacheHelper;
        }

        /**
         * use the same lock in the RolapCubeMemberSource as the
         * RolapCubeHierarchyMemberReader to avoid deadlocks
         */
        public Object getMemberCacheLock() {
            return memberCacheLock;
        }
    }

    /**
     * Implementation of {@link mondrian.rolap.RolapCubeMember} that wraps
     * a {@link mondrian.rolap.RolapStoredMeasure} and also implements that
     * interface.
     *
     * @see Util#deprecated(Object, boolean) TODO: Obsolete this class;
     *   {@link mondrian.rolap.RolapBaseCubeMeasure} already implements
     *   {@code mondrian.rolap.RolapMemberInCube} because a measure cannot be
     *   shared between cubes. Probably current code requires it to extend
     *   {@link mondrian.rolap.RolapCubeMember}.
     */
    public static class RolapCubeStoredMeasure
        extends RolapCubeMember
        implements RolapStoredMeasure
    {
        private final RolapStoredMeasure storedMeasure;

        public RolapCubeStoredMeasure(
            RolapCubeMember parent,
            RolapStoredMeasure member,
            RolapCubeLevel cubeLevel)
        {
            super(parent, (RolapMember) member, cubeLevel);
            this.storedMeasure = member;
            assert !(member instanceof RolapCubeMember) : member;
        }

        public RolapSchema.PhysExpr getExpr() {
            return storedMeasure.getExpr();
        }

        public RolapAggregator getAggregator() {
            return storedMeasure.getAggregator();
        }

        public RolapMeasureGroup getMeasureGroup() {
            return storedMeasure.getMeasureGroup();
        }

        public RolapResult.ValueFormatter getFormatter() {
            return storedMeasure.getFormatter();
        }

        public RolapStar.Measure getStarMeasure() {
            return storedMeasure.getStarMeasure();
        }
    }

    public static class RolapCubeCalculatedMeasure
        extends RolapCubeMember
        implements RolapMeasure, CalculatedMember
    {
        private final RolapCalculatedMeasure calculatedMeasure;

        public RolapCubeCalculatedMeasure(
            RolapCubeMember parent,
            RolapCalculatedMeasure member,
            RolapCubeLevel cubeLevel)
        {
            super(parent, member, cubeLevel);
            this.calculatedMeasure = member;
        }

        public RolapResult.ValueFormatter getFormatter() {
            return calculatedMeasure.getFormatter();
        }

        public Formula getFormula() {
            return calculatedMeasure.getFormula();
        }
    }
}

// End RolapCubeHierarchy.java
