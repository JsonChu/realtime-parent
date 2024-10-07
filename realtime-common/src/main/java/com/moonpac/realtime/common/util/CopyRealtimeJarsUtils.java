package com.moonpac.realtime.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class CopyRealtimeJarsUtils {

    public static void main(String[] args) {
        String destinationDirectoryPath = "D:\\公司\\海力士部署\\20240418-release-2.1.0"; // 修改为你的目标文件夹路径
        // 调用方法
        copyRealtimeJarsRecursive(destinationDirectoryPath,"realtime-common","realtime-dwd-sqlserver-vdi-latest-status");
    }

    private static void copyRealtimeJarsRecursive(String destinationPath, String... dynamicPath) {
        String sourceDirectoryPath = System.getProperty("user.dir"); // 获取当前工作目录

        File sourceDirectory = new File(sourceDirectoryPath);
        File destinationDirectory = new File(destinationPath);

        if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
            log.error("Source directory does not exist or is not a directory.");
            return;
        }
        if (!destinationDirectory.exists()) {
            destinationDirectory.mkdirs();
        }

        try {
            deleteRealtimeJarFiles(destinationPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 调用递归方法，仅复制文件而不复制目录结构
        copyRealtimeJarsRecursive(sourceDirectory, destinationDirectory, dynamicPath);
    }

    public static void deleteRealtimeJarFiles(String directoryPath) throws IOException {
        Path directory = Paths.get(directoryPath);
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file) && file.getFileName().toString().startsWith("realtime") && file.getFileName().toString().endsWith(".jar")) {
                    Files.delete(file);
                    log.info("删除旧的部署jar: " + file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void copyRealtimeJarsRecursive(File sourceDirectory, File destinationDirectory, String... dynamicPath) {
        File[] files = sourceDirectory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // 如果是目录，递归调用自身
                    copyRealtimeJarsRecursive(file, destinationDirectory, dynamicPath);
                } else {
                    // 如果是文件，复制到目标目录中
                    if (file.getName().startsWith("realtime") && file.getName().endsWith(".jar")) {
                        try {
                            Path sourceFilePath = file.toPath();
                            Path destinationFilePath = new File(destinationDirectory, file.getName()).toPath();
                            List<String> dynamicList = Arrays.asList(dynamicPath);
                            if (dynamicList.size() > 0){
                                for (String dynamicPathStr : dynamicList) {
                                    if (sourceFilePath.toString().contains(dynamicPathStr)){
                                        Files.copy(sourceFilePath, destinationFilePath, StandardCopyOption.REPLACE_EXISTING);
                                        log.info("copy:{},到指定的文件：{}",file.getName(),destinationFilePath.toString());
                                    }
                                }
                            }else{
                                Files.copy(sourceFilePath, destinationFilePath, StandardCopyOption.REPLACE_EXISTING);
                                log.info("copy:{},到指定的文件：{}",file.getName(),destinationFilePath.toString());
                            }
                        } catch (IOException e) {
                            log.error("Failed to copy file: " + file.getName());
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
