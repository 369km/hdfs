package com.dx.service;

import java.util.List;

/**
 * @Description
 * @Date 2020/11/18 上午10:31
 * @Created by yangfudong
 */
public interface HDFSService {

    /**
     * 遍历目录
     */
    List<String> listFiles(String dst);


    /**
     * 创建目录
     *
     * @param path 相对于根目录的目录
     */
    void mkdirs(String path);

    /**
     * 下载
     *
     * @param src 源目录
     * @param dst 目标目录
     */
    void copyToLocalFile(String src, String dst);


    /**
     * 上传
     *
     * @param src 源目录
     * @param dst 目标目录
     */
    void copyFromLocalFile(String src, String dst);


    /**
     * 合并小文件并上传
     *
     * @param src     源目录
     * @param dstFile 目标文件
     */
    void mergeFile(String src, String dstFile);
}
