package org.sngroup.util.CopyHelper;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.IdentityHashMap;
import java.util.Map;

// 通过反射dfs递归遍历对象的所有内容字段和引用字段,进行深拷贝
public class ReflectDeepCopy {

    // 用于跟踪已经拷贝的对象，防止重复拷贝和处理循环引用
    private Map<Object, Object> copiedObjects = new IdentityHashMap<>();

    public Object deepCopy(Object original) throws Exception {
        if (original == null) {
            return null;
        }

        // 如果已经拷贝过该对象，直接返回拷贝的对象
        if (copiedObjects.containsKey(original)) {
            return copiedObjects.get(original);
        }

        Class<?> originalClass = original.getClass();

        // 要注意分情况讨论, 在JAVA中无明确内存概念, 不同数据类型有不同处理方式
        // 如果是数组，单独处理(关键处理, Clone接口不能实现深拷贝的原因, 或在于此)
        if (originalClass.isArray()) {
            int length = Array.getLength(original);
            Object copyArray = Array.newInstance(originalClass.getComponentType(), length);
            copiedObjects.put(original, copyArray);
            for (int i = 0; i < length; i++) {
                Array.set(copyArray, i, deepCopy(Array.get(original, i)));
            }
            return copyArray;
        }

        // 如果是枚举或者基本类型包装类，直接返回原始对象
        if (originalClass.isEnum() || isWrapperType(originalClass)) {
            return original;
        }

        // 创建新的对象实例
        Object copyObject = originalClass.getConstructor().newInstance();
        copiedObjects.put(original, copyObject);

        // 递归拷贝所有字段
        while (originalClass != null) {
            for (Field field : originalClass.getDeclaredFields()) {
                field.setAccessible(true);
                // 跳过静态和瞬态字段 (关键处理, Clone接口不能实现深拷贝的原因, 或在于没有丢弃顺态字段)
                if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                    continue;
                }
                Object fieldValue = field.get(original);
                Object copiedFieldValue = deepCopy(fieldValue);
                field.set(copyObject, copiedFieldValue);
            }
            originalClass = originalClass.getSuperclass();
        }
        return copyObject;
    }

    private boolean isWrapperType(Class<?> clazz) {
        return clazz.equals(Boolean.class) || clazz.equals(Integer.class) || clazz.equals(Character.class) ||
                clazz.equals(Byte.class) || clazz.equals(Short.class) || clazz.equals(Double.class) ||
                clazz.equals(Long.class) || clazz.equals(Float.class);
    }
}