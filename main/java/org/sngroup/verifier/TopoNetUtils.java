package org.sngroup.verifier;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * TopoNet类的扩展，添加安全的startCount方法
 */
public class TopoNetUtils {
    
    /**
     * 安全地执行TopoNet的startCount方法，确保在处理完后将BDD引擎返回池中
     * 
     * @param topoNet 要执行的TopoNet实例
     * @param sharedQueueBDD BDD引擎池
     */
    public static void startCountSafe(TopoNet topoNet, LinkedBlockingDeque<BDDEngine> sharedQueueBDD) {
        BDDEngine originalEngine = topoNet.bddEngine;
        boolean success = false;
        
        try {
            // 执行原始的startCount
            topoNet.startCount(sharedQueueBDD);
            success = true;
        } finally {
            // 确保在任何情况下，包括异常，都将BDD引擎返回到池中
            if (originalEngine != null) {
                try {
                    // 先检查引擎是否还在topoNet对象中
                    if (!success || topoNet.bddEngine == originalEngine) {
                        sharedQueueBDD.offer(originalEngine);
                        System.out.println("安全地将BDD引擎归还到池中");
                    }
                } catch (Exception e) {
                    System.err.println("归还BDD引擎时发生错误: " + e.getMessage());
                }
            }
        }
    }
}