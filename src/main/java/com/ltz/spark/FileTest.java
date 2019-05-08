package com.ltz.spark;

import java.io.*;
import java.nio.file.*;

public class FileTest {

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis() ;
        Path path = Paths.get("/tmp/lineitem5.csv");
        //FileInputStream in = new FileInputStream(path.toFile());
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(path.toFile()));
        byte[] bytes = new byte[1024*1024];
        while (in.read(bytes,0,bytes.length)!=-1);
        System.out.println("end :"+(System.currentTimeMillis()-start));
    }
}
