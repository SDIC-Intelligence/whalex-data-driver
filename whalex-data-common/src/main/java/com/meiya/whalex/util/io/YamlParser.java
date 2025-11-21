package com.meiya.whalex.util.io;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

/**
 * yaml 解析工具类
 *
 * @author 黄河森
 * @date 2019/12/6
 * @project whale-cloud-platformX
 */
public class YamlParser {

    private static final Yaml yaml = new Yaml();

    /**
     * 解析 yaml 字符串
     * @param yamlStr
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T parserYaml(String yamlStr, Class<T> clazz) {
        return yaml.loadAs(yamlStr, clazz);
    }

    /**
     * 解析 yaml 文件
     * @param inputStream
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T parserYaml(InputStream inputStream, Class<T> clazz) {
        return yaml.loadAs(inputStream, clazz);
    }

}
