package com.moonpac.realtime.common.util;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import lombok.extern.slf4j.Slf4j;
import com.moonpac.realtime.common.bean.vcenter.AutoTelegrafConfig;

import java.io.*;

@Slf4j
public class FreeMarkerUtils {

    public static void deleteFilesWithPrefixAndSuffix(String directoryPath, String prefix, String suffix,String contains) {
        // 创建一个文件对象表示目录
        File directory = new File(directoryPath);

        // 检查目录是否存在
        if (directory.exists() && directory.isDirectory()) {
            // 创建一个FilenameFilter来选择以指定前缀开头且以指定后缀结尾的文件
            FilenameFilter filter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().startsWith(prefix.toLowerCase()) &&
                            name.toLowerCase().endsWith(suffix.toLowerCase()) &&
                            name.toLowerCase().contains(contains.toLowerCase());
                }
            };
            // 获取目录下所有符合条件的文件
            File[] files = directory.listFiles(filter);
            // 删除符合条件的文件
            if (files != null) {
                for (File file : files) {
                    if (file.delete()) {
                        log.info("删除之前存在的telegraf配置文件: {}", file.getName());
                    }
                }
            }
        }
    }

    public static void fillTemplate(String templatePath, String templateName,
                                    String outputPath, String outputFileName, AutoTelegrafConfig autoTelegrafConfig) {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
        try {
            // 设置模板文件所在目录
            cfg.setDirectoryForTemplateLoading(new File(templatePath));
            // 加载模板文件
            Template template = cfg.getTemplate(templateName);
            // 检查输出路径是否存在，如果不存在则创建它
            File directory = new File(outputPath);
            if (!directory.exists()) {
                if (!directory.mkdirs()) { // 创建目录失败
                    log.error("无法创建目录: {}", outputPath);
                    return;
                }
                log.info("创建目录成功: {}", outputPath);
            }
            // 指定输出文件的路径和名称
            String path = outputPath + File.separator +outputFileName;
            // 创建输出文件
            File outputFile = new File(path);
            if (outputFile.exists()) { // 文件已存在
                log.warn("文件已存在，info: {}", outputFile.getName());
            }
            Writer fileWriter = new FileWriter(outputFile);
            // 填充模板并输出到文件
            template.process(autoTelegrafConfig, fileWriter);
            // 关闭Writer
            fileWriter.close();
            log.info("生成配置文件成功 vcenter={},生成的配置文件={}",autoTelegrafConfig.getVcenter(),path);
        } catch (IOException | TemplateException e) {
            log.error("模板填充并写入文件时发生异常", e);
            throw new RuntimeException();
        }
    }
}
