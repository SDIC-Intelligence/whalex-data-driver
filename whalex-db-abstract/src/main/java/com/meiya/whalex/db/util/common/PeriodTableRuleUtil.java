package com.meiya.whalex.db.util.common;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.interior.db.constant.PeriodCycleEnum;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 欧阳少强
 * @date 2023/4/11
 * @package com.meiya.whalex.db.util.common
 * @project whalex-data-driver
 */
public class PeriodTableRuleUtil {
  
    public static final String FORMAT_YEAR = "_yyyy";
	public static final String FORMAT_MONTH = "_yyyyMM";
	public static final String FORMAT_DAY = "_yyyyMMdd";

	/**
	 * 压缩索引
	 * 不知道会被拿去干嘛，不使用 static，自己 new 吧
	 */
	public static String compress(String index, DateTime startTime, DateTime endTime, PeriodCycleEnum period) {

		// 基础校验
		if (StringUtils.isBlank(index) || Objects.isNull(startTime) || Objects.isNull(endTime)) {
			 throw new BusinessException("周期表名称压缩参数缺失!");
		}

		// 开始压缩
		List<DateTime> dateTimes;
		switch (period) {
			case DAY:
				dateTimes = DateUtil.rangeToList(DateUtil.beginOfDay(startTime), DateUtil.beginOfDay(endTime), DateField.DAY_OF_MONTH);
				return compressByDay(dateTimes, startTime, endTime, index);
			case MONTH:
				dateTimes = DateUtil.rangeToList(DateUtil.beginOfMonth(startTime), DateUtil.beginOfMonth(endTime), DateField.MONTH);
				return compressByMonth(dateTimes, startTime, endTime, index);
			case YEAR:
				dateTimes = DateUtil.rangeToList(DateUtil.beginOfYear(startTime), DateUtil.beginOfYear(endTime), DateField.YEAR);
				return compressByYear(dateTimes, startTime, endTime, index);
			default:
				return index;
		}
	}

	/**
	 *
	 *
	 * @param dateTimes
	 * @param startTime
	 * @param endTime
	 * @param index
	 * @return
	 */
	private static String compressByYear(List<DateTime> dateTimes, DateTime startTime, DateTime endTime, String index) {
		// 存放最终解析索引名
		Set<String> indices = new HashSet<>();
		dateTimes.forEach(dateTime -> {
			indices.add(index + changeFormatToYear(dateTime));
		});
		// 排序，拼接，返回
		return indices.stream()
				.sorted((o1, o2) -> StringUtils.compare(o1, o2))
				.collect(Collectors.joining(","));
	}

	/**
	 * 根据月份压缩
	 *
	 * @param dateTimes
	 * @param startTime
	 * @param endTime
	 * @param index
	 * @return
	 */
	private static String compressByMonth(List<DateTime> dateTimes, DateTime startTime, DateTime endTime, String index) {
		// 安装 年、月 两层规划日期
		Map<Integer, Map<Integer, DateTime>> dateMap = new HashMap<>();
		dateTimes.forEach(date -> {
			int year = DateUtil.year(date);
			int month = DateUtil.month(date); // 这里的1月是0，不过没关系，都一样
			dateMap.putIfAbsent(year, new HashMap<>());
			dateMap.get(year).put(month, date);
		});
		// 存放最终解析索引名
		Set<String> indices = new HashSet<>();
		dateMap.forEach((year, monthMap) -> {

			DateTime _rMonthDay = getRandomFromDay(monthMap);

			if (!inSameYear(_rMonthDay, startTime) && !inSameYear(_rMonthDay, endTime)
					|| (12 == monthMap.size())) {
				indices.add(index + changeBlurToYear(_rMonthDay));
			} else {
				monthMap.forEach((month, _rMonth) -> {
					indices.add(index + changeFormatToMonth(_rMonth));
				});
			}
		});
		// 排序，拼接，返回
		return indices.stream()
				.sorted((o1, o2) -> StringUtils.compare(o1, o2))
				.collect(Collectors.joining(","));
	}

	/**
	 * 按天压缩索引列表
	 *
	 * @param dateTimes
	 * @param startTime
	 * @param endTime
	 * @param index
	 * @return
	 */
	private static String compressByDay(List<DateTime> dateTimes, DateTime startTime, DateTime endTime, String index) {
		// 安装 年、月、日 三层规划日期
		Map<Integer, Map<Integer, Map<Integer, DateTime>>> dateMap = new HashMap<>();
		dateTimes.forEach(date -> {
			int year = DateUtil.year(date);
			int month = DateUtil.month(date); // 这里的1月是0，不过没关系，都一样
			int day = DateUtil.dayOfMonth(date);
			dateMap.putIfAbsent(year, new HashMap<>());
			dateMap.get(year).putIfAbsent(month, new HashMap<>());
			dateMap.get(year).get(month).put(day, date);
		});
		// 存放最终解析索引名
		Set<String> indices = new HashSet<>();
		dateMap.forEach((year, monthMap) -> {

			DateTime _rMonthDay = getRandomFromMonth(monthMap);

			if (!inSameYear(_rMonthDay, startTime) && !inSameYear(_rMonthDay, endTime)) {
				indices.add(index + changeBlurToYear(_rMonthDay));
			} else {
				monthMap.forEach((month, dayMap) -> {

					DateTime _rDay = getRandomFromDay(dayMap);

					// 判断不与起始时间和结束时间同一个月份的，或者月份天数完整的则压缩为月
					if ((!inSameMonth(_rDay, startTime) && !inSameMonth(_rDay, endTime))
							|| (getMonthDayNum(_rDay) == dayMap.size())) {
						indices.add(index + changeBlurToMonth(_rDay));
					} else {
						// 若不符合，则按天拆分
						dayMap.forEach((day, dateTime) -> {
							indices.add(index + changeFormatToDay(dateTime));
						});
					}
				});
			}
		});
		// 排序，拼接，返回
		return indices.stream()
				.sorted((o1, o2) -> StringUtils.compare(o1, o2))
				.collect(Collectors.joining(","));
	}

	/**
	 * 是否在同一年
	 *
	 * @param d1
	 * @param d2
	 * @return
	 */
	private static boolean inSameYear(DateTime d1, DateTime d2) {
		if (DateUtil.year(d1) == DateUtil.year(d2)) {
			return true;
		}
		return false;
	}

	/**
	 * 是否在同一年+同一月
	 *
	 * @param d1
	 * @param d2
	 * @return
	 */
	private static boolean inSameMonth(DateTime d1, DateTime d2) {
		if (DateUtil.year(d1) == DateUtil.year(d2)) {
			if (DateUtil.month(d1) == DateUtil.month(d2)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 返回 map 中随便一条
	 *
	 * @param map
	 * @return
	 */
	private static DateTime getRandomFromDay(Map<Integer, DateTime> map) {
		if (CollUtil.isNotEmpty(map)) {
			return map.values().stream().findFirst().orElse(null);
		}
		return null;
	}

	/**
	 * 返回 map 中随便一条
	 *
	 * @param map
	 * @return
	 */
	private static DateTime getRandomFromMonth(Map<Integer, Map<Integer, DateTime>> map) {
		if (CollUtil.isNotEmpty(map)) {
			return getRandomFromDay(map.values().stream().findFirst().orElse(null));
		}
		return null;
	}

	/**
	 * 返回某一天所在的月份有几天
	 *
	 * @param date
	 * @return
	 */
	private static int getMonthDayNum(DateTime date) {
		return DateUtil.dayOfMonth(date);
	}

	/**
	 * 转换为年份的索引
	 *
	 * @param date
	 * @return
	 */
	private static String changeBlurToYear(DateTime date) {
		return DateUtil.format(date, FORMAT_YEAR) + "*";
	}

	/**
	 * 转换为月份的索引
	 *
	 * @param date
	 * @return
	 */
	private static String changeBlurToMonth(DateTime date) {
		return DateUtil.format(date, FORMAT_MONTH) + "*";
	}

	/**
	 * 转换为某月的索引
	 *
	 * @param date
	 * @return
	 */
	private static String changeFormatToMonth(DateTime date) {
		return DateUtil.format(date, FORMAT_MONTH);
	}

	/**
	 * 转换为某年的索引
	 *
	 * @param date
	 * @return
	 */
	private static String changeFormatToYear(DateTime date) {
		return DateUtil.format(date, FORMAT_YEAR);
	}

	/**
	 * / 转换为某天的索引
	 *
	 * @param date
	 * @return
	 */
	private static String changeFormatToDay(DateTime date) {
		return DateUtil.format(date, FORMAT_DAY);
	}
  
}
