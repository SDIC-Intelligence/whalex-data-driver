package com.meiya.whalex.jdbc.factory;


import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ArrayUtil;
import com.meiya.whalex.annotation.DatabaseName;
import com.meiya.whalex.annotation.DbType;
import com.meiya.whalex.annotation.ExtendField;
import com.meiya.whalex.annotation.Host;
import com.meiya.whalex.annotation.Password;
import com.meiya.whalex.annotation.Port;
import com.meiya.whalex.annotation.Url;
import com.meiya.whalex.annotation.UserName;
import com.meiya.whalex.db.template.BaseDbConfTemplate;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.util.io.ScanJarClassUtil;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库配置信息工厂
 *
 * @author 蔡荣桂
 * @date 2022/06/27
 * @project whalex-dat-sql
 */
public class DatabaseConfFactory {

    private static Map<DbResourceEnum, Class<?>> dbType2DatabaseConfTemplate;

    private DatabaseConfFactory(){}


    public static void init() throws IOException, ClassNotFoundException {

        dbType2DatabaseConfTemplate = new HashMap<>();

        List<Class<?>> classList = ScanJarClassUtil.getClassList("com.meiya.whalex.db.template");

        for (Class<?> aClass : classList) {

            //获取类的注解
            Annotation[] annotations = aClass.getAnnotations();

            if (annotations.length == 0) {
                continue;
            }

            for (Annotation annotation : annotations) {

                if (annotation instanceof DbType) {
                    DbType dbType = (DbType) annotation;

                    DbResourceEnum[] value = dbType.value();

                    for (DbResourceEnum dbResourceEnum : value) {
                        dbType2DatabaseConfTemplate.put(dbResourceEnum, aClass);
                    }

                }

            }

        }

    }


    public static BaseDbConfTemplate getInstance(DbResourceEnum dbResourceEnum, Map<String, Object> params) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

        if(dbResourceEnum == null) {
            throw new RuntimeException("数据库类型不能为空");
        }

        if(CollectionUtil.isEmpty(params)) {
            throw new RuntimeException("参数不能为空");
        }

        if(dbType2DatabaseConfTemplate == null) {
            throw new RuntimeException("DatabaseConfFactory 厂场未初始化");
        }

        Class<?> aClass = dbType2DatabaseConfTemplate.get(dbResourceEnum);

        if(aClass == null) {
            throw new RuntimeException("未知的数据库类型:" + dbResourceEnum.name());
        }

        Object instance = aClass.newInstance();

        Field[] declaredFields = aClass.getDeclaredFields();

        Field[] baseDeclaredFields = aClass.getSuperclass().getDeclaredFields();

        declaredFields = ArrayUtil.append(declaredFields, baseDeclaredFields);

        for (Field declaredField : declaredFields) {


            declaredField.setAccessible(true);

            Class<?> type = declaredField.getType();

            Annotation[] annotations = declaredField.getAnnotations();
            if(annotations.length == 0) {
                continue;
            }

            Annotation annotation = annotations[0];

            if(annotation instanceof DatabaseName) {
                declaredField.set(instance, cast(params.get("db"), type));
            }else if(annotation instanceof ExtendField) {
                ExtendField extendField = (ExtendField) annotation;
                Object o = params.get(extendField.value());

                if (o != null) {

                    Class<?> declaringClass = declaredField.getType();
                    if(declaringClass.isEnum()) {
                        Method method = declaringClass.getMethod("getEnumByValue", o.getClass());
                        o = method.invoke(declaringClass, o);
                    }

                    declaredField.set(instance, cast(o, type));
                }
            }else if(annotation instanceof Host) {
                declaredField.set(instance, cast(params.get("host"), type));
            }else if(annotation instanceof Password) {
                declaredField.set(instance, cast(params.get("password"), type));
            }else if(annotation instanceof Port) {
                declaredField.set(instance, cast(params.get("port"), type));
            }else if(annotation instanceof Url) {
                declaredField.set(instance, cast(params.get("url"), type));
            }else if(annotation instanceof UserName) {
                declaredField.set(instance, cast(params.get("userName"), type));
            }

        }

        return (BaseDbConfTemplate) instance;
    }

    private static Object cast(Object value, Class classType) {

        if(classType.equals(boolean.class) && (value == null || value.toString().equals(""))) {
            return false;
        }

        if(value == null) {
            return null;
        }

        if(value.getClass().equals(classType)) {
            return value;
        }

        String str = value.toString();

        if(classType.equals(String.class)) {
           return str;
        }

        if(classType.equals(Long.class)) {
            return Long.valueOf(str);
        }

        if(classType.equals(Integer.class)) {
            return Integer.valueOf(str);
        }


        if(classType.equals(Boolean.class) || classType.equals(boolean.class)) {
            return Boolean.valueOf(str);
        }

        if(classType.isEnum()) {
           return  Enum.valueOf(classType, str);
        }

        throw new RuntimeException("暂不支持" + classType.getName() + "数据类型转换");
    }

//    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
//        init();
//        getInstance(DbResourceEnum.mysql, null);
//    }

}
