package com.dx.service;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2020/11/18 上午10:32
 * @Created by yangfudong
 */

@Service
public class HDFSServiceImpl implements HDFSService {

    private FileSystem getFileSystem() {
        FileSystem fileSystem = null;
        try {

            fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.1.108:9000"), new Configuration(), "hadoop");
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    /**
     * 遍历目录
     */
    public List<String> listFiles(String dst) {
        List<String> list = new ArrayList<>();
        try {
            FileSystem fileSystem = getFileSystem();
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(dst), true);
            while (iterator.hasNext()) {
                list.add(iterator.next().getPath().toString());
            }
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }


    /**
     * 创建目录
     *
     * @param path 相对于根目录的目录
     */
    public void mkdirs(String path) {
        try {
            FileSystem fileSystem = getFileSystem();
            fileSystem.mkdirs(new Path(path));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载
     *
     * @param src 源目录
     * @param dst 目标目录
     */
    public void copyToLocalFile(String src, String dst) {
        try {
            FileSystem fileSystem = getFileSystem();
            fileSystem.copyToLocalFile(new Path(src), new Path(dst));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 上传
     *
     * @param src 源目录
     * @param dst 目标目录
     */
    public void copyFromLocalFile(String src, String dst) {
        try {
            FileSystem fileSystem = getFileSystem();
            fileSystem.copyFromLocalFile(new Path(src), new Path(dst));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 合并小文件并上传
     *
     * @param src     源目录
     * @param dstFile 目标文件
     */
    public void mergeFile(String src, String dstFile) {
        try {
            FileSystem fileSystem = getFileSystem();
            FSDataOutputStream outputStream = fileSystem.create(new Path(dstFile));
            LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
            FileStatus[] fileStatuses = localFileSystem.listStatus(new Path(src));
            for (FileStatus f : fileStatuses) {
                FSDataInputStream inputStream = localFileSystem.open(f.getPath());
                IOUtils.copy(inputStream, outputStream);
                IOUtils.closeQuietly(inputStream);
            }
            IOUtils.closeQuietly(outputStream);
            localFileSystem.close();
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
