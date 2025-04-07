//package org.sngroup.test.runner;
//
//import java.io.IOException;
//import java.io.OutputStream;
//
//// 实现TeeOutputStream类，用于同时输出到两个流中
//public class TeeOutputStream extends OutputStream {
//    private OutputStream out1;
//    private OutputStream out2;
//
//    public TeeOutputStream(OutputStream out1, OutputStream out2) {
//        this.out1 = out1;
//        this.out2 = out2;
//    }
//
//    @Override
//    public void write(int b) throws IOException {
//        out1.write(b);
//        out2.write(b);
//    }
//}
