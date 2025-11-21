package com.meiya.whalex.util.io;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.system.OsInfo;
import com.meiya.whalex.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipFile;

/**
 * 扫描 jar 中符合路径和注解的 class 对象
 *
 * @author 黄河森
 * @date 2020/1/17
 * @project whale-cloud-platformX
 */
@Slf4j
public class ScanJarClassUtil {

    private static final String FILE_SUFFIX = ".class";

    /**
     * 获取指定包下包含指定注解的所有类对象的集合
     *
     * Spring环境下不可用
     *
     * @param pkgPath           包路径
     * @param targetAnnotations 指定注解
     * @return 以注解和对象类集合构成的键值对
     */
    public static Map<Class<? extends Annotation>, Set<Class<?>>> scanClassesByPackAndAnnotations(String pkgPath
            , List<Class<? extends Annotation>> targetAnnotations) throws IOException {

        List<String> allClassName = getAllClassName(pkgPath);
        return scanClassesByAnnotations(allClassName, targetAnnotations);
    }

    /**
     *
     * 获取指定全类名中含有指定注解的所有类对象的集合
     *
     * @param allClassName
     * @param targetAnnotations
     * @return
     * @throws IOException
     */
    public static Map<Class<? extends Annotation>, Set<Class<?>>> scanClassesByAnnotations(List<String> allClassName
            , List<Class<? extends Annotation>> targetAnnotations) throws IOException {
        if (CollectionUtil.isEmpty(allClassName)) {
            throw new BusinessException("not found class!");
        }
        Map<Class<? extends Annotation>, Set<Class<?>>> resultMap = new HashMap<>(16);
        allClassName.forEach(className -> {
            try {
                Class<?> curClass = getClassObj(className);
                for (Class<? extends Annotation> annotation : targetAnnotations) {
                    if (curClass.isAnnotationPresent(annotation)) {
                        if (!resultMap.containsKey(annotation)) {
                            resultMap.put(annotation, new HashSet<>());
                        }
                        resultMap.get(annotation).add(curClass);
                    }
                }
            } catch (ClassNotFoundException e) {
                log.error("load class fail!", e);
            }
        });
        return resultMap;
    }

    /**
     * 加载类
     *
     * @param className
     * @return
     * @throws ClassNotFoundException
     */
    private static Class<?> getClassObj(String className) throws ClassNotFoundException {
        className = StringUtils.replaceEach(className, new String[]{"/", FILE_SUFFIX}, new String[]{".", ""});
        return ScanJarClassUtil.class.getClassLoader().loadClass(className);
    }

    /**
     * 获取指定目录下的，符合条件的文件对象
     *
     * @param pkgPath
     * @return
     */
    public static List<String> getAllClassName(String pkgPath) throws IOException {
        List<String> classNameList = new LinkedList<>();
        Enumeration<URL> resources = ScanJarClassUtil.class.getClassLoader().getResources(pkgPath);
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            String file = url.getFile();
            if (new OsInfo().isLinux()) {
                file = StringUtils.replaceEach(file, new String[]{"!/" + pkgPath, "file:"}, new String[]{"", ""});
            } else {
                file = StringUtils.replaceEach(file, new String[]{"!/" + pkgPath, "file:/"}, new String[]{"", ""});
            }

            JarFile jarFile = null;

            try {
                jarFile = new JarFile(new File(StringUtils.replace(file, "/", File.separator)));
                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry jarEntry = entries.nextElement();
                    String name = jarEntry.getName();
                    if (StringUtils.endsWithIgnoreCase(name, FILE_SUFFIX)) {
                        classNameList.add(name);
                    }
                }
            }finally {
                if(jarFile != null) jarFile.close();
            }
        }
        return classNameList;
    }

    public static List<Class<?>> getClassList(String packageName) throws IOException, ClassNotFoundException {

        List<Class<?>> classes = new ArrayList<>();

        String packageDirName = packageName.replace(".", "/");

        Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(packageDirName);
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            String protocol = url.getProtocol();

            if(protocol.equals("file")) {
                String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                classes.addAll(findClassByDirectory(packageName, filePath));
            }else if(protocol.equals("jar")){
                classes.addAll(findClassInJar(packageName, url));
            }

        }
        return classes;
    }

    private static Collection<? extends Class<?>> findClassInJar(String packageName, URL url) throws IOException, ClassNotFoundException {

        List<Class<?>> classes = new ArrayList<>();

        String packageDirName = packageName.replace('.', '/');
        JarFile jarFile = ((JarURLConnection) url.openConnection()).getJarFile();
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()){
            JarEntry jarEntry = entries.nextElement();

            if(jarEntry.isDirectory()) {
                continue;
            }

            String name = jarEntry.getName();

            if(name.charAt(0) == '/') {
                name = name.substring(1);
            }

            if(name.startsWith(packageDirName)) {
                int index = name.lastIndexOf('/');
                if(index != -1) {
                    packageName = name.substring(0, index).replace('/', '.');
                    String className = name.substring(packageName.length() + 1, name.length() - 6);
                    Class<?> aClass = Class.forName(packageName + "." + className);
                    classes.add(aClass);
                }

            }
        }
        return classes;
    }

    private static Collection<? extends Class<?>> findClassByDirectory(String packageName, String filePath) throws ClassNotFoundException {

        List<Class<?>> classes = new ArrayList<>();

        File dir = new File(filePath);
        if(!dir.exists() || !dir.isDirectory()) {
            return classes;
        }

        File[] dirs = dir.listFiles();

        for (File file : dirs) {

            String fileName = file.getName();

            if(file.isDirectory()) {
                classes.addAll(findClassByDirectory(packageName + "." + fileName, file.getAbsolutePath()));
            }else if(fileName.endsWith(FILE_SUFFIX)) {
                String className = fileName.substring(0, fileName.length() - 6);
                Class<?> aClass = Class.forName(packageName + "." +  className);
                classes.add(aClass);
            }
        }

        return classes;
    }

}
