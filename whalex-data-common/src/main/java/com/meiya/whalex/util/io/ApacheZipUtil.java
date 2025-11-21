package com.meiya.whalex.util.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 类描述...
 *
 * @author huangzr
 * @date 2014-8-8
 */
@Slf4j
public class ApacheZipUtil {

    Map<String, byte[]> arrayByteZipMap = new HashMap<String, byte[]>();

    InputStream zipFile;

    private boolean isAll = true;
	/**
	 * 	需要的文件的正则表达式
	 */
    private String regex;

    private boolean isNeedFileName(String fileName) {
        Matcher matcher = Pattern.compile(regex).matcher(fileName);
        return matcher.find();
    }

    public ApacheZipUtil(InputStream inputStream) throws FileNotFoundException {
        this.zipFile = inputStream;
        this.isAll = true;
        initZipFile2ArrayBtyeMap();
    }

    public ApacheZipUtil(InputStream inputStream, String regex) throws FileNotFoundException {
        this.zipFile = inputStream;
        this.isAll = false;
        this.regex = regex;
        initZipFile2ArrayBtyeMap();
    }

    public ApacheZipUtil(File zipFile, String regex) throws FileNotFoundException {
        this.zipFile = new FileInputStream(zipFile);
        this.isAll = false;
        this.regex = regex;
        initZipFile2ArrayBtyeMap();
    }

    public ApacheZipUtil(String zipFilePath) throws FileNotFoundException {
        this.zipFile = new FileInputStream(new File(zipFilePath));
        this.isAll = true;
        initZipFile2ArrayBtyeMap();
    }

    public ApacheZipUtil(String zipFilePath, String regex) throws FileNotFoundException {
        this.zipFile = new FileInputStream(new File(zipFilePath));
        this.isAll = false;
        this.regex = regex;
        initZipFile2ArrayBtyeMap();
    }


    public List<String> getAllNamesFromZip() {
        List<String> fileNamelist = new ArrayList<String>(arrayByteZipMap.size());
        fileNamelist.addAll(arrayByteZipMap.keySet());
        return fileNamelist;
    }

    public InputStream getInputStream(String fileName) {
        if (getFileByte(fileName) == null) {
            return null;
        }
        return new ByteArrayInputStream(getFileByte(fileName));
    }

    public byte[] getFileByte(String fileName) {
        return arrayByteZipMap.get(fileName);
    }


    public void destroy() {
        arrayByteZipMap.clear();
    }

    private static byte[] toByteArray(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        IOUtils.copy(input, output);
        return output.toByteArray();
    }


    public Map<String, byte[]> getArrayByteZipMap() {
        return arrayByteZipMap;
    }


    private void initZipFile2ArrayBtyeMap() {
        ZipArchiveInputStream is = null;
        try {
            is = new ZipArchiveInputStream(zipFile);
            ZipArchiveEntry entry = null;
            while ((entry = (ZipArchiveEntry) is.getNextEntry()) != null) {
                if (isAll || isNeedFileName(entry.getName())) {
                    arrayByteZipMap.put(entry.getName(), toByteArray(is));
                }
            }
        } catch (Exception e) {
            log.error("解压{}出错", zipFile, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
