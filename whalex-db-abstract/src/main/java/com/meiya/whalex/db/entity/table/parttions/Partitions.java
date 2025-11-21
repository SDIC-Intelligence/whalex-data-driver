/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.meiya.whalex.db.entity.table.parttions;

import java.io.Serializable;
import java.util.*;

import cn.hutool.core.collection.CollectionUtil;
import com.meiya.whalex.db.entity.table.expressions.Expression;
import com.meiya.whalex.db.entity.table.expressions.literals.Literal;
import com.meiya.whalex.interior.db.constant.PartitionTypeV2Constants;

/** The helper class for partition expressions. */
public class Partitions {

  /** An empty array of partitions. */
  public static Partition[] EMPTY_PARTITIONS = new Partition[0];

  /**
   * Creates a range partition.
   *
   * @param name The name of the partition.
   * @param upper The upper bound of the partition.
   * @param lower The lower bound of the partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static RangePartition range(
      String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties,List<Expression> keys) {
    return new RangePartitionImpl(name, upper, lower, properties,keys);
  }

  public static RangeColumnsPartition rangeColumns(
          String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties,List<Expression> keys) {
    return new RangeColumnsPartitionImpl(name, upper, lower, properties,keys);
  }

  /**
   * Creates a list partition.
   *
   * @param name The name of the partition.
   * @param lists The values of the list partition.
   * @param properties The properties of the partition.
   * @return The created partition.
   */
  public static ListPartition list(
      String name, Literal<?>[][] lists, Map<String, String> properties,List<Expression> keys) {
    return new ListPartitionImpl(name, lists, properties,keys);
  }

  public static ListColumnsPartition listColumns(
          String name, Literal<?>[][] lists, Map<String, String> properties,List<Expression> keys) {
    return new ListColumnsPartitionImpl(name, lists, properties,keys);
  }

  public static KeyPartition key(
          String name,  Map<String, String> properties,List<Expression> keys) {
    return new KeyPartitionImpl(name, properties,keys);
  }

  public static HashPartition hash(
          String name,  Map<String, String> properties,List<Expression> keys) {
    return new HashPartitionImpl(name, properties,keys);
  }







  /** Represents a result of range partitioning. */
  private static class RangePartitionImpl implements RangePartition {
    private final String name;

    private final String parttionType;

    private final List<Expression> keys;

    private final Literal<?> upper;
    private final Literal<?> lower;

    private final Map<String, String> properties;

    private RangePartitionImpl(
        String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties,List<Expression> keys) {
      this.name = name;
      this.properties = properties;
      this.upper = upper;
      this.lower = lower;
      this.parttionType = PartitionTypeV2Constants.RANGE;
      this.keys =keys;
    }

    /** @return The upper bound of the partition. */
    @Override
    public Literal<?> upper() {
      return upper;
    }

    /** @return The lower bound of the partition. */
    @Override
    public Literal<?> lower() {
      return lower;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String parttionType() {
      return parttionType;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public List<Expression> keys() {
      return keys;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RangePartitionImpl)) {
        return false;
      }
      RangePartitionImpl that = (RangePartitionImpl) o;
      return Objects.equals(name, that.name)
          && Objects.equals(upper, that.upper)
          && Objects.equals(lower, that.lower)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, upper, lower, properties);
    }

    @Override
    public Map<String, Object> toMap() {
      Map<String,Object> map = new HashMap<>();
      map.put("name",name);
      map.put("upper",upper);
      map.put("lower",lower);
      map.put("properties",properties);
      map.put("parttionType",parttionType);
      map.put("keys",keys);
      return map;
    }
  }

  /** Represents a result of list partitioning. */
  private static class ListPartitionImpl implements ListPartition {
    private final String name;



    private final String parttionType;
    private final Literal<?>[][] lists;

    private final Map<String, String> properties;

    private final List<Expression> keys;

    private ListPartitionImpl(String name, Literal<?>[][] lists, Map<String, String> properties,List<Expression> keys) {
      this.name = name;
      this.properties = properties;
      this.lists = lists;
      this.parttionType = PartitionTypeV2Constants.LIST;
      this.keys=keys;
    }

    /** @return The values of the list partition. */
    @Override
    public Literal<?>[][] lists() {
      return lists;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String parttionType() {
      return parttionType;
    }

    @Override
    public List<Expression> keys() {
      return keys;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ListPartitionImpl)) {
        return false;
      }
      ListPartitionImpl that = (ListPartitionImpl) o;
      return Objects.equals(name, that.name)
          && Arrays.deepEquals(lists, that.lists)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, properties);
      result = 31 * result + Arrays.deepHashCode(lists);
      return result;
    }
    @Override
    public Map<String, Object> toMap() {
      Map<String,Object> map = new HashMap<>();
      map.put("name",name);
      map.put("lists",lists);
      map.put("parttionType",parttionType);
      map.put("properties",properties);
      map.put("keys",keys);
      return map;
    }
  }

  private static class HashPartitionImpl implements HashPartition {
    private final String name;

    private final String parttionType;

    private final List<Expression> keys;

    private final Map<String, String> properties;

    private HashPartitionImpl(String name, Map<String, String> properties,List<Expression> keys) {
      this.name = name;
      this.properties = properties;
      this.parttionType = PartitionTypeV2Constants.HASH;
      this.keys= keys;
    }

    /** @return The values of the list partition. */


    @Override
    public String name() {
      return name;
    }

    @Override
    public String parttionType() {
      return parttionType;
    }

    @Override
    public List<Expression> keys() {
      return keys;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ListPartitionImpl)) {
        return false;
      }
      ListPartitionImpl that = (ListPartitionImpl) o;
      return Objects.equals(name, that.name)
              && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, properties);
      result = 31 * result ;
      return result;
    }
    @Override
    public Map<String, Object> toMap() {
      Map<String,Object> map = new HashMap<>();
      map.put("name",name);
      map.put("parttionType",parttionType);
      map.put("properties",properties);
      map.put("keys",keys);
      map.put("parttionType",parttionType);
      return map;
    }
  }

  private static class KeyPartitionImpl implements KeyPartition {
    private final String name;

    private final String parttionType;

    private final Map<String, String> properties;

    private final List<Expression> keys;

    private KeyPartitionImpl(String name, Map<String, String> properties,List<Expression> keys) {
      this.name = name;
      this.properties = properties;
      this.parttionType = PartitionTypeV2Constants.KEY;
      this.keys=keys;
    }

    /** @return The values of the list partition. */


    @Override
    public String name() {
      return name;
    }

    @Override
    public List<Expression> keys() {
      return keys;
    }

    @Override
    public String parttionType() {
      return parttionType;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ListPartitionImpl)) {
        return false;
      }
      ListPartitionImpl that = (ListPartitionImpl) o;
      return Objects.equals(name, that.name)
              && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, properties);
      result = 31 * result ;
      return result;
    }
    @Override
    public Map<String, Object> toMap() {
      Map<String,Object> map = new HashMap<>();
      map.put("name",name);
      map.put("parttionType",parttionType);
      map.put("properties",properties);
      map.put("keys",keys);
      map.put("parttionType",parttionType);
      return map;
    }
  }


  private static class ListColumnsPartitionImpl implements ListColumnsPartition {
    private final String name;



    private final String parttionType;
    private final Literal<?>[][] lists;

    private final Map<String, String> properties;

    private final List<Expression> keys;

    private ListColumnsPartitionImpl(String name, Literal<?>[][] lists, Map<String, String> properties,List<Expression> keys) {
      this.name = name;
      this.properties = properties;
      this.lists = lists;
      this.parttionType = PartitionTypeV2Constants.LIST_COLUMNS;
      this.keys=keys;
    }

    /** @return The values of the list partition. */
    @Override
    public Literal<?>[][] lists() {
      return lists;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String parttionType() {
      return parttionType;
    }

    @Override
    public List<Expression> keys() {
      return keys;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ListPartitionImpl)) {
        return false;
      }
      ListPartitionImpl that = (ListPartitionImpl) o;
      return Objects.equals(name, that.name)
              && Arrays.deepEquals(lists, that.lists)
              && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, properties);
      result = 31 * result + Arrays.deepHashCode(lists);
      return result;
    }
    @Override
    public Map<String, Object> toMap() {
      Map<String,Object> map = new HashMap<>();
      map.put("name",name);
      map.put("lists",lists);
      map.put("parttionType",parttionType);
      map.put("properties",properties);
      map.put("keys",keys);
      return map;
    }
  }
  /** Represents a result of identity partitioning. */

  private static class RangeColumnsPartitionImpl implements RangeColumnsPartition {
    private final String name;

    private final String parttionType;

    private final List<Expression> keys;

    private final Literal<?> upper;
    private final Literal<?> lower;

    private final Map<String, String> properties;

    private RangeColumnsPartitionImpl(
            String name, Literal<?> upper, Literal<?> lower, Map<String, String> properties,List<Expression> keys) {
      this.name = name;
      this.properties = properties;
      this.upper = upper;
      this.lower = lower;
      this.parttionType = PartitionTypeV2Constants.RANGE_COLUMNS;
      this.keys =keys;
    }

    /** @return The upper bound of the partition. */
    @Override
    public Literal<?> upper() {
      return upper;
    }

    /** @return The lower bound of the partition. */
    @Override
    public Literal<?> lower() {
      return lower;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String parttionType() {
      return parttionType;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public List<Expression> keys() {
      return keys;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RangePartitionImpl)) {
        return false;
      }
      RangePartitionImpl that = (RangePartitionImpl) o;
      return Objects.equals(name, that.name)
              && Objects.equals(upper, that.upper)
              && Objects.equals(lower, that.lower)
              && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, upper, lower, properties);
    }

    @Override
    public Map<String, Object> toMap() {
      Map<String,Object> map = new HashMap<>();
      map.put("name",name);
      map.put("upper",upper);
      map.put("lower",lower);
      map.put("properties",properties);
      map.put("parttionType",parttionType);
      map.put("keys",keys);
      return map;
    }
  }


  private Partitions() {}
}
