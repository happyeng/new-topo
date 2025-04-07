/*
 * This program is free software: you can redistribute it and/or modify it under the terms of
 *  the GNU General Public License as published by the Free Software Foundation, either
 *   version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *   PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this
 *  program. If not, see <https://www.gnu.org/licenses/>.
 *
 * Authors: Chenyang Huang (Xiamen University) <xmuhcy@stu.xmu.edu.cn>
 *          Qiao Xiang     (Xiamen University) <xiangq27@gmail.com>
 *          Ridi Wen       (Xiamen University) <23020211153973@stu.xmu.edu.cn>
 *          Yuxin Wang     (Xiamen University) <yuxxinwang@gmail.com>
 */

 package org.sngroup.test.runner;

 import org.sngroup.Configuration;
 import org.sngroup.util.*;
 import org.sngroup.verifier.*;

 import java.io.*;
 import java.lang.management.ThreadMXBean;
 import java.net.UnknownHostException;
 import java.util.*;
 import java.util.concurrent.ExecutorService;
 import java.util.concurrent.Executors;
 import java.util.concurrent.LinkedBlockingDeque;
 import java.util.concurrent.TimeUnit;
 import java.util.concurrent.atomic.AtomicInteger;

 public class TopoRunner extends Runner {

     private static final int THREAD_POOL_READ_SIZE = 1; // 设置线程池大小
     public ThreadPool threadPool;
     public static Map<String, Device> devices;

     public int ruleCnt = 0;

     public static boolean isIpv6 = false;
     public static boolean isIpv4withS = true;

     // BDD池的大小，根据系统资源调整
     private int bddPoolSize = 50;
     // 每批处理的topoNet数量
     private int batchSize = 500000;

     private static Map<String, TopoNet> topoNetMap;
     private static Map<Integer, HashSet<TopoNet>> topoNetGroupMap = new HashMap<>();
     Set<Integer> dvNetSet;

     public static DVNet srcNet;
     public static BDDEngine srcBdd;

     // 性能监控服务实例
     private MonitorService monitor;

     public TopoRunner(){
         super();
         devices = new HashMap<>();
         dvNetSet = new HashSet<>();
         topoNetMap = new HashMap<>();
         monitor = MonitorService.getInstance();
     }

     public ThreadPool getThreadPool(){
         return threadPool;
     }
     public Device getDevice(String name){
         return devices.get(name);
     }

     public void build(){
         // 开始构建阶段监控
         monitor.startBuildPhase();

         // IPV6 OR IPV4
         if(isIpv6) BDDEngine.ipBits = 128;
         else BDDEngine.ipBits = 32;
         srcBdd = new BDDEngine();

         System.out.println("Start Build in Runner!!!");
         srcNet = new DVNet(-1, srcBdd);
         threadPool = ThreadPool.FixedThreadPool(Configuration.getConfiguration().getThreadPoolSize());
         devices.clear();
         // 读取所有的device
         for(String deviceName : network.devicePorts.keySet()){
             Device d = new Device(deviceName, network, this, threadPool);
             if(network.edgeDevices.contains(deviceName)){
                 TopoNet.edgeDevices.add(d);
             }
             devices.put(deviceName, d);
         }
         // device读取规则
         if(isIpv6 || isIpv4withS) readRuleByDeviceIPV6();
         else readRuleByDevice();
         // srcBDD转化规则
         srcBddTransformAllRules();
         // 生成topoNet
         genTopoNet();
         System.out.println("结点总数量" + devices.size());
         System.out.println("S0结点数量" + network.edgeDevices.size());
         System.out.println("表项总数量" + ruleCnt);

         // 设置BDD池大小和批处理大小
         calcBddPoolAndBatchSize();

         System.out.println("End Build in Runner!!");

         // 结束构建阶段监控
         monitor.endBuildPhase();
     }

     // 计算合适的BDD池大小和每批处理的topoNet数量
     private void calcBddPoolAndBatchSize() {
         // 根据硬件资源和任务数量计算BDD池大小
         int availableProcessors = Runtime.getRuntime().availableProcessors();
         bddPoolSize = Math.min(availableProcessors, topoNetMap.size() / 2);
         bddPoolSize = Math.max(1, bddPoolSize); // 至少1个

         // 使用原始默认值
         batchSize = 500000;

         System.out.println("BDD池大小: " + bddPoolSize);
         System.out.println("每批处理的topoNet数量: " + batchSize);
     }

     public void start() {
         // 开始验证阶段监控
         monitor.startVerifyPhase();

         AtomicInteger netCNt = new AtomicInteger(1);
         LinkedBlockingDeque<BDDEngine> sharedQueueBDD = new LinkedBlockingDeque<>();

         // 初始化BDD引擎池
         initBddPool(sharedQueueBDD);

         // 将topoNet分批处理
         processByBatches(sharedQueueBDD);

         // 结束验证阶段监控
         monitor.endVerifyPhase();

         // 输出性能总结
         monitor.printSummary();
     }

     // 初始化BDD引擎池
     private void initBddPool(LinkedBlockingDeque<BDDEngine> sharedQueueBDD) {
         System.out.println("初始化BDD引擎池，大小: " + bddPoolSize);
         for (int i = 0; i < bddPoolSize; i++) {
             try {
                 // 创建基于源BDD的新引擎实例
                 BDDEngine bddEngine = new BDDEngine(srcBdd, true);
                 if (bddEngine != null && bddEngine.bdd != null) {
                     sharedQueueBDD.put(bddEngine);
                     System.out.println("创建BDD引擎 #" + (i+1) + " 成功");
                 } else {
                     System.err.println("创建BDD引擎 #" + (i+1) + " 失败");
                     i--; // 重试
                 }
             } catch (Exception e) {
                 System.err.println("初始化BDD引擎失败: " + e.getMessage());
                 e.printStackTrace();
                 i--; // 重试
             }
         }
         System.out.println("BDD引擎池初始化完成，当前大小: " + sharedQueueBDD.size());
     }

     // 批量处理topoNet
     private void processByBatches(LinkedBlockingDeque<BDDEngine> sharedQueueBDD) {
         List<List<Map.Entry<String, TopoNet>>> batches = createBatches(topoNetMap, batchSize);

         System.out.println("处理 " + batches.size() + " 批topoNet，每批最多 " + batchSize + " 个");

         for (int i = 0; i < batches.size(); i++) {
             List<Map.Entry<String, TopoNet>> batch = batches.get(i);
             System.out.println("处理第 " + (i+1) + "/" + batches.size() + " 批，包含 " + batch.size() + " 个topoNet");

             // 开始监控这个批次
             monitor.startBatchMonitoring(i+1, batch.size());

             // 处理单个批次
             processBatch(batch, sharedQueueBDD);

             // 确保当前批次完成后再处理下一批
             threadPool.awaitAllTaskFinished();

             // 结束这个批次的监控
             monitor.endBatchMonitoring(i+1);

             System.out.println("第 " + (i+1) + " 批处理完成");

             // 触发手动GC以便清理资源
             if (i < batches.size() - 1) {
                 System.gc();
                 try {
                     Thread.sleep(1000); // 给GC一些时间
                 } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                 }
             }
         }
     }

     // 处理单个批次的topoNet
     private void processBatch(List<Map.Entry<String, TopoNet>> batch, LinkedBlockingDeque<BDDEngine> sharedQueueBDD) {
         for (Map.Entry<String, TopoNet> entry : batch) {
             threadPool.execute(() -> {
                 TopoNet topoNet = entry.getValue();
                 String topoNetId = entry.getKey();

                 try {
                     // 开始初始化监控
                     monitor.startTopoNetInitialization(topoNetId);

                     // 获取BDD引擎（复用现有的或创建新的）
                     long waitStartTime = System.currentTimeMillis();
                     boolean reused = topoNet.getAndSetBddEngine(sharedQueueBDD);
                     long waitTime = System.currentTimeMillis() - waitStartTime;
                     monitor.recordBDDWaitTime(waitTime);

                     if (reused) {
                         System.out.println("TopoNet " + entry.getKey() + " 复用BDD引擎");
                     } else {
                         System.out.println("TopoNet " + entry.getKey() + " 创建新的BDD引擎");
                         // 如果无法从池中获取，创建新的BDD引擎
                         BDDEngine newEngine = new BDDEngine(srcBdd, true);
                         topoNet.bddEngine = newEngine;
                     }

                     // 生成节点
                     topoGenNode(topoNet);

                     // 设置节点的BDD（复用或从新创建）
                     topoNet.setNodeBdd();

                     // 设置包空间
                     String dstDevice = topoNet.dstDevice.name;
                     if (DVNet.devicePacketSpace.containsKey(dstDevice)) {
                         int s = DVNet.devicePacketSpace.get(dstDevice);
                         topoNet.setPacketSpace(s);
                     }

                     // 设置设备的Lec集合
                     topoNet.deviceLecs = Device.globalLecs;

                     // 计算节点入度
                     topoNet.nodeCalIndegree();

                     // 获取BDD节点数量
                     int bddNodesCount = getBddNodesCount(topoNet);

                     // 结束初始化监控
                     monitor.endTopoNetInitialization(topoNetId, bddNodesCount, reused);

                     // 开始计算监控
                     monitor.startTopoNetComputation(topoNetId);

                     // 记录开始使用BDD的时间
                     long usageStartTime = System.currentTimeMillis();

                     // 执行验证并在完成后返回BDD引擎到池中
                     try {
                         topoNet.startCount(sharedQueueBDD);
                     } finally {
                         // 确保BDD引擎被归还到池中
                         if (topoNet.bddEngine != null) {
                             try {
                                 sharedQueueBDD.put(topoNet.bddEngine);
                                 System.out.println("归还BDD引擎到池中");
                             } catch (InterruptedException e) {
                                 Thread.currentThread().interrupt();
                                 System.err.println("归还BDD引擎时被中断");
                             }
                         }
                     }

                     // 记录BDD使用时间
                     long usageTime = System.currentTimeMillis() - usageStartTime;
                     monitor.recordBDDUsageTime(usageTime);

                     // 再次获取BDD节点数量，观察计算过程可能增加的节点
                     bddNodesCount = getBddNodesCount(topoNet);

                     // 结束计算监控
                     monitor.endTopoNetComputation(topoNetId, bddNodesCount);

                 } catch (Exception e) {
                     System.err.println("处理TopoNet " + topoNetId + " 时发生错误: " + e.getMessage());
                     e.printStackTrace();

                     // 确保异常情况下也释放资源
                     if (topoNet.bddEngine != null) {
                         try {
                             sharedQueueBDD.put(topoNet.bddEngine);
                             System.out.println("异常情况下归还BDD引擎");
                         } catch (InterruptedException ie) {
                             Thread.currentThread().interrupt();
                         }
                     }
                 }
             });
         }
     }

     // 获取BDD节点数量
     private int getBddNodesCount(TopoNet topoNet) {
         if (topoNet == null || topoNet.bddEngine == null || topoNet.bddEngine.bdd == null) {
             return 0;
         }

         try {
             // 这里根据实际情况获取BDD节点数
             return topoNet.bddEngine.bdd.nodeCount(1); // 使用TRUE节点作为根获取总节点数
         } catch (Exception e) {
             System.err.println("获取BDD节点数量失败: " + e.getMessage());
             return 0;
         }
     }

     // 将topoNetMap分成多个批次
     private List<List<Map.Entry<String, TopoNet>>> createBatches(Map<String, TopoNet> topoNetMap, int batchSize) {
         List<List<Map.Entry<String, TopoNet>>> batches = new ArrayList<>();
         List<Map.Entry<String, TopoNet>> entries = new ArrayList<>(topoNetMap.entrySet());

         for (int i = 0; i < entries.size(); i += batchSize) {
             int end = Math.min(i + batchSize, entries.size());
             batches.add(entries.subList(i, end));
         }

         return batches;
     }

     private void genTopoNet() {
         topoNetMap = new HashMap<>();
         int topoCnt = -1;
         // 根据edgeDevices初始化topoNet对象并设置device
         TopoNet.network = this.network;
         for(Device dstDevice : TopoNet.edgeDevices){
             if(!network.dstDevices.contains(dstDevice.name)) continue;
             TopoNet topoNet = new TopoNet(dstDevice, topoCnt);
             topoNet.setInvariant(dstDevice.name, "exist >= 1", "*");
             topoNetMap.put(dstDevice.name, topoNet);
             topoCnt--;
         }
         TopoNet.transformDevicePorts(network.devicePorts);
         TopoNet.setNextTable();
     }

     private void readRuleByDevice(){
         // 先从文件中读取规则, 并插入规则
         for (Map.Entry<String, Device> entry : devices.entrySet()) {
             threadPool.execute(() -> {
                 String name = entry.getKey();
                 Device device = entry.getValue();
                 device.readOnlyRulesFile(Configuration.getConfiguration().getDeviceRuleFile(name));
             });
         }
         // 读取网段
         Device.readOnlySpaceFile(Configuration.getConfiguration().getSpaceFile());
         threadPool.awaitAllTaskFinished();
     }

     private void readRuleByDeviceIPV6(){
         // 先从文件中读取规则, 并插入规则
         for (Map.Entry<String, Device> entry : devices.entrySet()) {
             threadPool.execute(() -> {
                 String name = entry.getKey();
                 Device device = entry.getValue();
                 if(isIpv4withS){
                    device.readOnlyRulesFileIPV4_S(Configuration.getConfiguration().getDeviceRuleFile(name));
                 }else{
                    device.readOnlyRulesFileIPV6(Configuration.getConfiguration().getDeviceRuleFile(name));
                 }

             });
         }
         // 读取网段
         Device.readOnlySpaceFileIPV6(Configuration.getConfiguration().getSpaceFile());
         threadPool.awaitAllTaskFinished();
     }

     public void srcBddTransformAllRules(){
        // transformRuleWithTrie();  // 目前只支持ipv4下的十进制读取，即该分支
        // transformRuleWithTrieIPV6(); // IPV6 unfinished
        transformRuleWithoutTrie(); // 仍然存在bug
     }

     public void transformRuleWithTrie(){
         long timePoint1 = System.currentTimeMillis();
         for(Device device : devices.values()){
             device.encodeDeviceRule(srcNet);
         }
         long timePoint2 = System.currentTimeMillis();
         System.out.println("规则转化所使用的时间" + (timePoint2 - timePoint1) + "ms");
         srcNet.srcDvNetParseAllSpace(Device.spaces);
         long timePoint3 = System.currentTimeMillis();
         System.out.println("BDD编码所使用的总时间" + (timePoint3 - timePoint1) + "ms");
     }

     public void transformRuleWithTrieIPV6(){
        long timePoint1 = System.currentTimeMillis();
        for(Device device : devices.values()){
            device.encodeDeviceRuleIPV6(srcNet);
        }
        long timePoint2 = System.currentTimeMillis();
        System.out.println("规则转化所使用的时间" + (timePoint2 - timePoint1) + "ms");
        srcNet.srcDvNetParseAllSpaceIPV6(Device.spacesIPV6);
        long timePoint3 = System.currentTimeMillis();
        System.out.println("BDD编码所使用的总时间" + (timePoint3 - timePoint1) + "ms");
    }

     public void transformRuleWithoutTrie(){
         long timePoint1 = System.currentTimeMillis();
         for(Device device : devices.values()){
             //TODO: 尚存在bug
 //            device.encodeRuleToLecFromScratchToFinish(srcNet);
             if(!(isIpv6 || isIpv4withS))device.encodeRuleToLecFromScratch(srcNet); // IPV4
             else {
                 try {
                     device.encodeRuleToLecFromScratchIPV6(srcNet); // IPV6
                 } catch (UnknownHostException e) {
                     throw new RuntimeException(e);
                 }
             }
             ruleCnt += device.rules.size();
         }
         long timePoint2 = System.currentTimeMillis();
         System.out.println("规则转化所使用的时间" + (timePoint2 - timePoint1) + "ms");
         if(!(isIpv6|| isIpv4withS))srcNet.srcDvNetParseAllSpace(Device.spaces);
         else srcNet.srcDvNetParseAllSpaceIPV6(Device.spacesIPV6);
         long timePoint3 = System.currentTimeMillis();
         System.out.println("BDD编码所使用的总时间" + (timePoint3 - timePoint1) + "ms");
     }

     private void topoGenNode(TopoNet topoNet){
         for(Device device : devices.values()){
 //            Node node = new Node(device, topoNet.topoCnt, topoNet.invariant, topoNet.runner);
             Node node = new Node(device, topoNet);
             topoNet.nodesTable.put(device.name, node);
             if(TopoNet.edgeDevices.contains(device)){
                 if(device == topoNet.dstDevice) { // 终结点
                     topoNet.setDstNode(node);
                     node.isDestination = true;
                 }
                 else{ // 边缘结点
                     topoNet.srcNodes.add(node);
                 }
             }
         }
     }

     public void topoNetDeepCopyBdd(TopoNet topoNet, boolean reused){
         String dstDevice = topoNet.dstDevice.name;
         int s = DVNet.devicePacketSpace.get(dstDevice);

         // 如果BDD引擎已被复用，只需设置节点的BDD
         if(reused) {
             topoNet.setNodeBdd();
         } else {
             // 否则需要复制BDD
             try {
                 topoNet.copyBdd(srcBdd, "Reflect");
             } catch (Exception e) {
                 System.err.println("复制BDD失败: " + e.getMessage());
                 throw new RuntimeException(e);
             }
         }

         topoNet.deviceLecs = Device.globalLecs;
         topoNet.setPacketSpace(s);
     }

     @Override
     public void awaitFinished(){
         threadPool.awaitAllTaskFinished(100);
     }

     @Override
     public void sendCount(Context ctx, DevicePort sendPort, BDDEngine bddEngine) {
         // 此方法在此实现中不需要具体实现
     }

     public long getInitTime(){
         return 0;
     }

     @Override
     public void close(){
         devices.values().forEach(Device::close);
         threadPool.shutdownNow();
     }
 }