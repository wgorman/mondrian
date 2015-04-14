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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.SqlConstraint;

/**
 * The cache key implementation used by SqlContextConstraint and its subclasses
 * as well as TupleReaders.
 * The key is a hash map with some basic key strings defined, as well as relevant
 * getters and setters. It is extensible in that classes can define their own key strings
 * and add contents directly to the key using these key strings.
 * CacheKeys can be created from this CacheKey by specifying the keys in the this key
 * to either include or exclude in the resulting CacheKey. This allows classes to compare
 * areas of the key they may not know about, as well as compare areas of the key they are
 * interested in.
 * <p/>
 * When keys are set using the public setValue method,
 * an existing value is not overwritten. Instead, if an existing value is found
 * and if the old value does not equal the new value and the old value
 * is not a list, a new list is
 * created and both the old and new values are added to it. If the existing value is a list
 * the new value is added to it. If the existing value is null, then the new value is inserted.
 * The map allows null values, but not null keys.
 * <p/>
 * The known keys for which there are getters and setters do not use this appending approach.
 * Instead, existing values are simply overwritten. The known values will typically not be
 * written to more than once, in particular with a different value. So overwriting an existing
 * value ensures the getters do not throw a class cast exception.
 *
 */
public class CacheKey {

    protected static final Logger LOGGER = Logger.getLogger(CacheKey.class);

    /**
     * Key used for the contraint class.
     */
    public static final String KEY_CONSTRAINT_CLASS = "mondrian.rolap.cachekey.constraint.class";
    /**
     * Key defining whether or not the constraint is strict.
     */
    public static final String KEY_STRICT = "mondrian.rolap.cachekey.strict";
    /**
     * List of members referenced by the evaluator.
     */
    public static final String KEY_MEMBERS = "mondrian.rolap.cachekey.members";
    /**
     * Tuple list referenced by evaluator.
     */
    public static final String KEY_SLICER_TUPLES = "mondrian.rolap.cachekey.slicer.tuples";

    public static final String KEY_SLICER_MEMBERS = "mondrian.rolap.cachekey.slicer.members";
    /**
     * List of access role members.
     */
    public static final String KEY_ROLE_MEMBER_LISTS = "mondrian.rolap.cachekey.role.member.lists";
    /**
     * List of RolapCubes referenced by the evaluator.
     */
    public static final String KEY_CUBES = "mondrian.rolap.cachekey.cubes";
    /**
     * List of CrossJoinArgs referenced by the constraint.
     */
    public static final String KEY_CROSSJOIN_ARGS = "mondrian.rolap.cachekey.crossjoin.args";


    private final Map<String, Object> contents = new HashMap<String, Object>();

    /**
     * Create an empty cache key
     */
    public CacheKey() {
    }

    /**
     * Create a cache key from a given key. This copies the contents of the
     * passed in key into this one.
     * @param key
     */
    public CacheKey(CacheKey key) {
        for (Map.Entry<String, Object> entry : key.contents.entrySet()) {
            this.contents.put(entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    public Class<? extends SqlConstraint> getConstraintClass() {
        return (Class<? extends SqlConstraint>) getValue(KEY_CONSTRAINT_CLASS);
    }

    public Object setConstraintClass(Class<? extends SqlConstraint> val) {
       return setValueInternal(KEY_CONSTRAINT_CLASS, val);
    }

    @SuppressWarnings ("unchecked")
    public Boolean getStrict() {
       return (Boolean) getValue(KEY_STRICT);
    }

    public Object setStrict(Boolean val) {
        return setValueInternal(KEY_STRICT, val);
    }

    @SuppressWarnings ("unchecked")
    public List<Member> getMembers() {
        return (List<Member>) getValue(KEY_MEMBERS);
    }

    public Object setMembers(List<Member> val) {
        return setValueInternal(KEY_MEMBERS, val);
    }

    @SuppressWarnings ("unchecked")
    public TupleList getSlicerTuples() {
        return (TupleList) getValue(KEY_SLICER_TUPLES);
    }

    public Object setSlicerTuples(TupleList val) {
        return setValueInternal(KEY_SLICER_TUPLES, val);
    }

    @SuppressWarnings ("unchecked")
    public List<List<RolapMember>> getRoleMemberLists() {
        return (List<List<RolapMember>>) getValue(KEY_ROLE_MEMBER_LISTS);
    }

    public Object setRoleMemberLists(List<List<RolapMember>> val) {
        return setValueInternal(KEY_ROLE_MEMBER_LISTS, val);
    }

    @SuppressWarnings ("unchecked")
    public List<RolapCube> getCubes() {
        return (List<RolapCube>) getValue(KEY_CUBES);
    }

    public Object setCubes(List<RolapCube> val) {
        return setValueInternal(KEY_CUBES, val);
    }

    @SuppressWarnings ("unchecked")
    public List<CrossJoinArg> getCrossJoinArgs() {
        return (List<CrossJoinArg>) getValue(KEY_CROSSJOIN_ARGS);
    }

    public Object setCrossJoinArgs(List<CrossJoinArg> val) {
        return setValueInternal(KEY_CROSSJOIN_ARGS, val);
    }

    @SuppressWarnings ("unchecked")
    public List<Member> getSlicerMembers() {
        return (List<Member>) getValue(KEY_SLICER_MEMBERS);
    }

    public Object setSlicerMembers(List<Member> val) {
        return setValueInternal(KEY_SLICER_MEMBERS, val);
    }

    public Object getValue(String key) {
        return contents.get(key);
    }

    /**
     * Sets a value, overwriting the existing value if it is present.
     * This is used for the 'known' keys, which should not appended to.
     * @param key  the key for the value
     * @param value the value.
     * @return the object that was stored at the key previously, or null.
     */
    private Object setValueInternal(String key, Object value) {
        assert key != null;
        Object ret = contents.put(key, value);
        if (ret != null && !ret.equals(value) && LOGGER.isTraceEnabled()) {
            LOGGER.trace("addValue KEY:" + key
                + " EXISTING VALUE:" + ret + " ADDED VALUE:" + value);
        }
        return ret;
    }

    /**
     * Should check for the types being added here, but that would mean
     * iterating through lists to check component types which is added overhead.
     * @param key  the key for the value
     * @param value the value.
     * @return the object that was stored at the key previously, or null.
     */
    public Object setValue(String key, Object value) {
        assert key != null;
        Object ret = contents.put(key, value);
        if(ret != null && !ret.equals(value)) {
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("addValue KEY:" + key
                    + " EXISTING VALUE:" + ret + " ADDED VALUE:" + value);
            }
            if(ret instanceof List) {
                ((List)ret).add(value);
            } else {
                List<Object> list = new ArrayList<Object>();
                list.add(ret);
                list.add(value);
                contents.put(key, list);
            }
        }
        return ret;
    }

    /**
     * Create a cache key from this cache key that contains
     * all values except those mapped to the passed
     * in key strings.
     * @param excludedKeys
     * @return
     */
    public CacheKey getCacheKeyWithoutKeys(String... excludedKeys) {
        CacheKey ret = new CacheKey();
        for (Map.Entry<String, Object> entry : contents.entrySet()) {
            boolean add = true;
            for (String excludedKey : excludedKeys) {
                if(excludedKey.equals(entry.getKey())) {
                    add = false;
                    break;
                }
            }
            if(add) {
                ret.setValue(entry.getKey(), entry.getValue());
            }
        }
        return ret;
    }

    /**
     * Create a cache key from this one that only contains values that map to the
     * passed in key strings.
     * @param includedKeys
     * @return
     */
    public CacheKey getCacheKeyWithKeys(String... includedKeys) {
        CacheKey ret = new CacheKey();
        for (Map.Entry<String, Object> entry : contents.entrySet()) {
            for (String includedKey : includedKeys) {
                if (includedKey.equals(entry.getKey())) {
                    ret.setValue(entry.getKey(), entry.getValue());
                    break;
                }
            }
        }
        return ret;
    }

    public int size() {
        return contents.size();
    }

    @Override
    public String toString() {
        return contents.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CacheKey)) {
            return false;
        }

        CacheKey cacheKey = (CacheKey) o;

        if (!contents.equals(cacheKey.contents)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return contents.hashCode();
    }
}
