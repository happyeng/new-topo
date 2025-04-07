//package org.sngroup.util.CopyHelper;
//
//import org.nustaq.serialization.FSTObjectInput;
//import org.nustaq.serialization.FSTObjectOutput;
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.Serializable;
//
//public class FSTDeepCopy {
//
//    // 使用FST进行深拷贝
//    public <T extends Serializable> T deepCopy(T object) {
//        try {
//            // 将对象序列化为字节数组
//            byte[] serializedBytes = serialize(object);
//
//            // 将字节数组反序列化为新的对象
//            return deserialize(serializedBytes);
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    // 使用FST进行对象序列化, 读时需要同步
//    static synchronized private byte[] serialize(Object object) throws IOException {
//        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//             FSTObjectOutput out = new FSTObjectOutput(byteArrayOutputStream)) {
//
//            out.writeObject(object);
//            out.flush();
//            return byteArrayOutputStream.toByteArray();
//        }
//    }
//
//    // 使用FST进行对象反序列化
//    private  <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
//        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
//             FSTObjectInput in = new FSTObjectInput(byteArrayInputStream)) {
//
//            return (T) in.readObject();
//        }
//    }
//}
