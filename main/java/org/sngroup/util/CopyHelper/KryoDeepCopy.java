package org.sngroup.util.CopyHelper;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import jdd.bdd.BDD;
import org.sngroup.verifier.BDDEngine;
import org.sngroup.verifier.TSBDD;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class KryoDeepCopy {

    public Kryo kryo = new Kryo();

    // 使用 Kryo 进行深拷贝
    public  <T extends Serializable> T deepCopy(T object) {

        // 注册您的自定义类（如果有的话）
//         kryo.register(YourClass.class);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream);
        kryo.writeObject(output, object);
        output.close();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        Input input = new Input(byteArrayInputStream);
        T copy = kryo.readObject(input, (Class<T>) object.getClass());
        input.close();

        return copy;
    }

    // 放入与BDD相关的所有数据结构
    public void kryoRegister(){
//        kryo.register(BDDEngine.class);
//        kryo.register(TSBDD.class);
//        kryo.register(BDD.class);
//        kryo.ge
////        kryo.register(.class);
        kryo.setRegistrationRequired(false);
        kryo.register(BDDEngine.class.getPackage().getClass());



    }



}
