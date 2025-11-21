package com.meiya.whalex.db.entity.graph;

/**
 * @author chenjp
 * @date 2/25/2021
 */
public interface GremlinFunction {

    String _V = "V(";
    String _E = "E(";
    String _AS = "as(";
    String _NULL = "";
    String _SELECT = "select(";
    String _EQ = "eq(";
    String _NEQ = "neq(";
    String _LT = "lt(";
    String _LTE = "lte(";
    String _GT = "gt(";
    String _GTE = "gte(";
    String _INSIDE = "insider(";
    String _OUTSIDE = "outside(";
    String _BETWEEN = "between(";
    String _WITHIN = "within(";
    String _WITHOUT = "without(";
    String _INCR = "incr";
    String _DECR = "decr";
    String _SHUFFLE = "shuffle";
    String _UNDER = "__";
    String _LAB = "lab";

    String _ID = "id(";
    String _LABEL = "label(";
    String _PROPERTIES = "properties(";
    String _KEY = "key(";
    String _VALUE = "value(";
    String _VALUE_MAP = "valueMap(";
    String _VALUES = "values(";

    String _IN = "in(";
    String _OUT = "out(";
    String _BOTH = "both(";
    String _OUT_E = "outE(";
    String _IN_E = "inE(";
    String _BOTH_E = "bothE(";
    String _OUT_V = "outV(";
    String _IN_V = "inV(";
    String _BOTH_V = "bothV(";
    String _OTHER_V = "otherV(";

    String _HAS_LABEL = "hasLabel(";
    String _HAS_ID = "hasId(";
    String _HAS = "has(";
    String _HAS_KEY = "hasKey(";
    String _HAS_VALUE = "hasValue(";
    String _HAS_NOT = "hasNot(";

    String _COUNT = "count(";
    String _RANGE = "range(";
    String _LIMIT = "limit(";
    String _TAIL = "tail(";
    String _SKIP = "skip(";

    String _PATH = "path(";
    String _SIMPLE_PATH = "simplePath(";
    String _CYCLIC_PATH = "cyclicPath(";

    String _REPEAT = "repeat(";
    String _TIMES = "times(";
    String _UNTIL = "until(";
    String _EMIT = "emit(";
    String _LOOPS = "loops(";

    String _ORDER = "order(";
    String _BY = "by(";

    String _GROUP = "group(";
    String _GROUP_COUNT = "groupCount(";
    String _DEDUP = "dedup(";

    String _WHERE = "where(";
    String _FILTER = "filter(";

    String _IS = "is(";
    String _AND = "and(";
    String _OR = "or(";
    String _NOT = "not(";

    String _SUM = "sum(";
    String _MAX = "max(";
    String _MIN = "min(";
    String _MEAN = "mean(";

    String _MATH = "math(";

    String _AS_SELECT = "asSelect(";
    String _AS_WHERE = "asWhere(";
    String _AS_MATCH = "asMatch(";
    String _AS_DEDUP = "asDedup(";

    String _CHOOSE = "choose(";
    String _BRANCH = "branch(";

    String _COALESCE = "coalesce(";
    String _OPTIONAL = "optional(";
    String _UNION = "union(";

    String _AGGREGATE = "aggregate(";
    String _STORE = "store(";
    String _UNFOLD = "unfold(";
    String _FOLD = "fold(";

    String _MATCH = "match(";

    String _SAMPLE = "sample(";
    String _COIN = "coin(";
    String _CONSTANT = "constant(";
    String _INJECT = "inject(";

    String _SACK = "sack(";

    String _BARRIER = "barrier(";

    String _LOCAL = "local(";

    String _HAS_NEXT = "hasNext(";
    String _NEXT = "next(";
    String _TRY_NEXT = "tryNext(";
    String _TO_LIST = "toList(";
    String _TO_SET = "toSet(";
    String _TO_BULK_SET = "toBulkSet(";
    String _FILL = "fill(";
    String _ITERATE = "iterate(";

    String _MAP = "map(";
    String _FLATMAP = "flatMap(";

    String _SIDE_EFFECT = "sideEffect(";

    String _PROFILE = "profile(";
    String _EXPLAIN = "explain(";

}
