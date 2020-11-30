package com.dx.controller;

import com.dx.service.HDFSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @Description HDFS API
 * @Date 2020/11/17 上午10:21
 * @Created by yangfudong
 */
@RestController
@RequestMapping("/hdfs")
public class HDFSController {
    @Autowired
    private HDFSService hdfsService;

    /**
     * 遍历目录
     */
    @GetMapping("/ll")
    public List<String> listFiles() {
        return hdfsService.listFiles("/");
    }


    /**
     * 创建目录
     *
     */
    @GetMapping("/mkdir")
    public void mkdirs() {
        hdfsService.mkdirs("/hdfs");
    }

    /**
     * 下载
     *
     */
    @GetMapping("/download")
    public void copyToLocalFile() {
        hdfsService.copyToLocalFile("/test", "/Users/yangfudong/download/");
    }


    /**
     * 上传
     *
     */
    @GetMapping("/upload")
    public void copyFromLocalFile() {
        hdfsService.copyFromLocalFile("/Users/yangfudong/download/10080_QYINFO_E_ALTER_RECODER_TS_20200522_20200529_D_00_0001.DAT.gz", "/hive");
    }

    /**
     * 合并小文件并上传
     *
     * @param src     源目录
     * @param dstFile 目标文件
     */
    @GetMapping("/merge")
    public void mergeFile(String src, String dstFile) {
        hdfsService.mergeFile(src, dstFile);
    }
}
