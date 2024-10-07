package com.moonpac.realtime.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public class FileUtils {

    /**
     * 将文件复制到指定目录
     *
     * @param sourceFilePath 源文件路径
     * @param targetDirPath  目标目录路径
     * @throws IOException 如果复制过程中出现 IO 异常
     */
    public static void copyFileToDirectory(String sourceFilePath, String targetDirPath) throws IOException {
        // 源文件路径
        File sourceFile = new File(sourceFilePath);
        // 目标目录路径
        File targetDir = new File(targetDirPath);

        // 检查源文件是否存在
        if (!sourceFile.exists()) {
            throw new IOException("源文件不存在: " + sourceFilePath);
        }

        // 检查目标目录是否存在，如果不存在则创建
        if (!targetDir.exists()) {
            boolean created = targetDir.mkdirs();
            if (!created) {
                throw new IOException("无法创建目标目录: " + targetDirPath);
            }
        }
        // 目标文件路径
        File targetFile = new File(targetDir, sourceFile.getName());
        // 使用 Files.copy 方法进行复制
        Files.copy(sourceFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    public static List<String> getCheckpoint (String filePath) throws IOException {

        // 检查文件夹是否存在，如果不存在则创建它
        File directory = new File(filePath);
        if (!directory.exists() && !directory.getParentFile().exists()) {
            directory.getParentFile().mkdirs();
        }
        // 创建文件
        File file = new File(filePath);
        if (!file.exists() && file.createNewFile()) {
            log.info("文件:{},已成功创建",file.getName());
        }

        List<String> dataList = new ArrayList<>();

        // 创建FileReader对象用于读取文件
        FileReader fileReader = new FileReader(filePath);

        // 创建BufferedReader对象用于读取文本
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        // 读取文件中的数据
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            dataList.add(line);
        }
        // 关闭读取流
        bufferedReader.close();

        return dataList;
    }

    public static void writeFile(String filePath, String data, boolean append, int maxLines) throws IOException {
        // 检查文件夹是否存在，如果不存在则创建它
        File directory = new File(filePath);
        if (!directory.getParentFile().exists()) {
            directory.getParentFile().mkdirs();
        }
        // 创建文件
        File file = new File(filePath);

        if (!file.exists() && file.createNewFile()) {
            System.out.println("文件:" + file.getName() + "，已成功创建");
        }

        // 如果文件存在并且行数超过最大行数，则删除前面几行
        if (file.exists()) {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            if (lines.size() + 1 > maxLines) {
                lines = lines.subList(lines.size() + 1 - maxLines, lines.size());
                Files.write(Paths.get(filePath), lines);
            }
        }

        // 创建BufferedWriter对象，用于写入文件
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, append))) {
            // 将输入的数据写入文件
            writer.write(data);
        }
    }

    public static void main(String[] args) throws IOException {
        // 测试
        String filePath = "D:\\tmp\\file\\result.txt";
        writeFile(filePath, "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n", true, 10);
        System.out.println("文件写入完成。");
    }

}
