package com.meiya.whalex.util.io;


import java.io.File;
import java.net.URL;

public class FindFileUtil {


    //从 class,userdir,conf,config 里面查找文件
    public static   String getFilePath(String file) {

        String configFile=System.getProperty("user.dir") + File.separator +file;
        if(new File(configFile).exists()) return configFile;

        configFile=System.getProperty("user.dir") + File.separator +"conf"+ File.separator +file;
        if(new File(configFile).exists()) return configFile;

        configFile=System.getProperty("user.dir") + File.separator +"config"+ File.separator +file;
        if(new File(configFile).exists()) return configFile;

        URL url= FindFileUtil.class.getClassLoader().getResource(file);
        if(url!=null) return new File(url.getPath()).getAbsolutePath();

        return null;
    }
}
