package com.meiya.whalex.db.resolver;

import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.impl.BeanConverter;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.exception.BusinessException;
import lombok.Data;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 数据结果解析器
 *
 * @author 黄河森
 * @date 2021/6/21
 * @project whalex-data-driver-back
 */
public class DataRowResolver {

    private PageResult pageResult;

    private DataRowResolver(PageResult pageResult) {
        this.pageResult = pageResult;
    }

    /**
     * 解析器
     *
     * @param pageResult
     * @return
     */
    public static DataRowResolver resolver(PageResult pageResult) {
        return new DataRowResolver(pageResult);
    }

    /**
     * 数据结果转换器
     *
     * @param tClass
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> DataRow<T> analysis(Class<T> tClass) throws Exception {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        if (CollectionUtil.isNotEmpty(this.pageResult.getRows())) {
            List<T> collect = (List<T>) this.pageResult.getRows().stream().flatMap(row -> {
                BeanConverter<T> converter = new BeanConverter<>(tClass, CopyOptions.create().setIgnoreError(false));
                T convert = converter.convert(row, null);
                return Stream.of(convert);
            }).collect(Collectors.toList());
            DataRow<T> dataRow = new DataRow<>();
            dataRow.setTotal(this.pageResult.getTotal());
            dataRow.setData(collect);
            return dataRow;
        } else {
            DataRow<T> dataRow = new DataRow<>();
            dataRow.setTotal(this.pageResult.getTotal());
            return dataRow;
        }
    }

    /**
     * 数据结果转换器
     *
     * @param tClass
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> SingleDataRow<T> analysisSingle(Class<T> tClass) throws Exception {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        if (CollectionUtil.isNotEmpty(this.pageResult.getRows())) {
            if (this.pageResult.getRows().size() > 1) {
                throw new BusinessException("parsed data is a single data but the actual result is greater than one!");
            } else {
                BeanConverter<T> converter = new BeanConverter<>(tClass, CopyOptions.create().setIgnoreError(false));
                T convert = converter.convert(this.pageResult.getRows().get(0), null);
                SingleDataRow<T> dataRow = new SingleDataRow<>();
                dataRow.setTotal(this.pageResult.getTotal());
                dataRow.setData(convert);
                return dataRow;
            }
        } else {
            SingleDataRow<T> dataRow = new SingleDataRow<>();
            dataRow.setTotal(this.pageResult.getTotal());
            return dataRow;
        }
    }

    /**
     * 自定义数据结果提取器
     *
     * @param resultsExtractor
     * @param <T>
     * @return
     */
    public <T> T analysis(ResultsExtractor<T> resultsExtractor) {
        if (!this.pageResult.getSuccess()) {
            throw new BusinessException(this.pageResult.getCode(), this.pageResult.getMessage());
        }
        return (T) resultsExtractor.extract(this.pageResult.getTotal(), this.pageResult.getRows());
    }

    /**
     * 数据结果转换对象
     *
     * @param <T>
     */
    @Data
    public static class DataRow<T> {
        private Long total;
        private List<T> data;
    }

    /**
     * 数据结果转换对象
     *
     * @param <T>
     */
    @Data
    public static class SingleDataRow<T> {
        private Long total;
        private T data;
    }

    public interface ResultsExtractor<T> {
        /**
         * 数据结果提取方法
         *
         * @param total
         * @param rows
         * @return
         */
        T extract(Long total, List<Map<String, Object>> rows);
    }
}
