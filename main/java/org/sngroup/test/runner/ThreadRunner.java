///*
// * This program is free software: you can redistribute it and/or modify it under the terms of
// *  the GNU General Public License as published by the Free Software Foundation, either
// *   version 3 of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY
// *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
// *   PARTICULAR PURPOSE. See the GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License along with this
// *  program. If not, see <https://www.gnu.org/licenses/>.
// *
// * Authors: Chenyang Huang (Xiamen University) <xmuhcy@stu.xmu.edu.cn>
// *          Qiao Xiang     (Xiamen University) <xiangq27@gmail.com>
// *          Ridi Wen       (Xiamen University) <23020211153973@stu.xmu.edu.cn>
// *          Yuxin Wang     (Xiamen University) <yuxxinwang@gmail.com>
// */
//
//package org.sngroup.test.runner;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import org.sngroup.Configuration;
//import org.sngroup.util.*;
//import org.sngroup.verifier.*;
//
//import java.io.*;
//import java.util.*;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicInteger;
//
//
//public class ThreadRunner extends Runner {
//    private static final int THREAD_POOL_READ_SIZE = 100; // 设置线程池大小
//    public ThreadPool threadPool;
//    // private final Serialization srl;
//    Map<String, Device> devices;
//
//    // 根据DVnet对Devcie进行划分
//    public static Map<Integer, DVNet> dvNetMap;
//    Set<Integer> dvNetSet;
//
//    public static DVNet srcNet;
//
//    public static BDDEngine srcBdd;
//
//    public boolean isCopyBdd = true;
//
//    public static int nodeCnt = 0;
//
//    Map<String, BufferedWriter> saveBW;
//    public ThreadRunner(){
//        super();
//
//        // srl = new ProtobufSerialization();
//        devices = new Hashtable<>();
//        dvNetMap = new HashMap<>();
//        dvNetSet = new HashSet<>();
//        srcBdd = new BDDEngine();
//
//    }
//
//    public ThreadPool getThreadPool(){
//        return threadPool;
//    }
//    public Device getDevice(String name){
//        return devices.get(name);
//    }
//
//    public void writeTime(long alltime, String filepath){
//        try{
//
//            String data = alltime +  "ms\n";
//
//            //true = append file
//            FileWriter fileWritter = new FileWriter(filepath, true);
//            fileWritter.write(data);
//            fileWritter.close();
////            System.out.println("Done");
//
//        }catch(IOException e){
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void build() {
//        System.out.println("Start build in Runner!!!!!");
//        srcNet = new DVNet(-1, srcBdd);
//        long timePoint1 = System.currentTimeMillis();
//        threadPool = ThreadPool.FixedThreadPool(Configuration.getConfiguration().getThreadPoolSize());
//        devices.clear();
//        int deviceCnt = network.devicePorts.entrySet().size();
//        // 按device 并行初始化
//        for (String deviceName : network.devicePorts.keySet()) {
//            threadPool.execute(() -> {
//                Device d = new Device(deviceName, network, this, threadPool);
//                srcNet.addDevice(d);
//                for (Map.Entry<Integer, VNode> entry : network.nodes.get(deviceName).entrySet()) {
//                    int index = entry.getKey();
//                    VNode vn = entry.getValue();
//                    Collection<NodePointer> nextSet = new LinkedList<>();
//                    Collection<NodePointer> prevSet = new LinkedList<>();
//                    for (VNode next : vn.next) {
//                        String nextDevice = next.device;
//                        for (DevicePort dp : network.devicePorts.get(deviceName).get(nextDevice)) {
//                            nextSet.add(new NodePointer(dp.getPortName(), next.index));
//                        }
//                    }
//                    for (VNode prev : vn.prev) {
//                        String prevDevice = prev.device;
//                        for (DevicePort dp : network.devicePorts.get(deviceName).get(prevDevice)) {
//                            prevSet.add(new NodePointer(dp.getPortName(), prev.index));
//                        }
//                    }
//                    d.addNode(index, prevSet, nextSet, vn.isEnd, vn.invariant, vn.netIndex);
//                    synchronized (dvNetMap){
//                        if(!dvNetMap.containsKey(vn.netIndex)) dvNetMap.put(vn.netIndex, new DVNet(vn.netIndex));
//                    }
//                }
//                devices.put(deviceName, d);
//            });
//        }
//        threadPool.awaitAllTaskFinished();
//
//        // 按net 并行分配node
//        for(Device d : devices.values()){
//            for(Node node : d.nodes.values()) {
//                // initialize dvnet
//                threadPool.execute(() -> {
//                    int netIndex = node.netIndex;
//                    DVNet dvNet = dvNetMap.get(netIndex);
//                    synchronized (dvNet) {
//                        node.setDvNet(dvNet);
//                        dvNet.addNode(node);
//                        dvNet.addDevice(d);
//                        if (node.isDestination) {
//                            dvNet.setDstNode(node);
//                            nodeCnt++;
//                        } else if (node.prev.isEmpty()) {
//                            dvNet.addSrcNode(node);
//                            nodeCnt++;
//                        }
//                    }
//                });
//            }
//        }
//        threadPool.awaitAllTaskFinished();
//
////        System.out.println("所有图的起点以及终点计数总和" + nodeCnt);
//        long timePoint2 = System.currentTimeMillis();
//        System.out.println("device总数:  " + deviceCnt);
//        System.out.println("node, device, dvnet初始化对象所花费的时间:  " + (timePoint2 - timePoint1)+"ms");
//        long stime = System.currentTimeMillis();
//        long timePoint3 = System.currentTimeMillis();
//        System.out.println("从文件中读取规则文件所花费的时间:  " + (timePoint3 - timePoint2)+"ms");
//        initializeByNet();
//        if(isCopyBdd){
////            initializeDevice();
//            srcBddTransformAllRUles();
//            dvNetCopyBdd();
//        }
//        else {
//            dvNetReadRulus();
//        }
//        long timePoint4 = System.currentTimeMillis();
//        System.out.println("node, device, dvnetBDD转化所花费的总时间:  " + (timePoint4 - timePoint3)+"ms");
//        long etime = System.currentTimeMillis();
//        String filepathi = "./result/initializeDevice.csv";
//        writeTime(etime - stime, filepathi);
//    }
//
//    private void initializeByNet(){
//        long builtTIme = 0;
//        // 先从文件中读取规则, 并插入规则
//        for (Map.Entry<String, Device> entry : devices.entrySet()) {
//        threadPool.execute(() -> {
//            String name = entry.getKey();
//            Device device = entry.getValue();
//            device.readOnlyRulesFile(Configuration.getConfiguration().getDeviceRuleFile(name));
//            device.readOnlySpaceFile(Configuration.getConfiguration().getSpaceFile());
//            device.addAllRule();
//        });
//        }
//        threadPool.awaitAllTaskFinished();
//    }
//
//    public void srcBddTransformAllRUles(){
//            for(Device device : devices.values()){
////            Device device = entry.getValue();
////                device.srcBddReadRules(srcBdd);
//                device.dvNetRepeat_new(srcNet);
////                device.dvNetParseSpace(Device.spacesTmp, srcNet);
//            }
//            srcNet.srcDvNetParseAllSpace(Device.spacesTmp);
//
//
//
//
//
//
//
////            for(Map.Entry<String, List<Lec>> entry : Device.globalLecs.entrySet()){
////                List<Lec> lecs = entry.getValue();
////                for(Lec lec : lecs){
////                    System.out.println(lec.type + lec.forwardAction.toString() + lec.predicate);
////                }
////            }
//            // 刷新输出流并关闭PrintStream
////            System.out.flush();
////            printStream.close();
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
////        for (Device device : devices.values()) {
////            device.dvNetRepeat(dvNet);
////            device.dvNetParseSpace(device.spacesTmp, dvNet);
////        }
//    }
//
//    public void dvNetCopyBdd(){
//        // BDD按图初始化
//        for(Map.Entry<Integer, DVNet> entry : dvNetMap.entrySet()){
//            DVNet dvNet = entry.getValue();
//            threadPool.execute(() -> {
////                long timePoint = System.currentTimeMillis();
//                String dstDevice = dvNet.getDstDeviceName();
//                int s = DVNet.devicePacketSpace.get(dstDevice);
//                // COPY TYPE : choose from "Reflect", "FST", "Kryo", "Apache".
//                try {
//                    dvNet.copyBdd(srcBdd, "Kryo");
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
////                long timePoint1 = System.currentTimeMillis();
////                System.out.println("copyBDD所用时间:  " + (timePoint1 - timePoint)+"ms");
//                dvNet.copyLecs(Device.globalLecs);
//                dvNet.setPacketSpace(s);
////                dvNet.dvNetParseSpace(Device.spacesTmp);
//            });
//        }
//        threadPool.awaitAllTaskFinished();
//    }
//
//    public void dvNetReadRulus(){
//        // BDD按图初始化
//        for(Map.Entry<Integer, DVNet> entry : dvNetMap.entrySet()){
//            threadPool.execute(() -> {
//            DVNet dvNet = entry.getValue();
//            Set<Device> deviceSet = dvNet.getDevices();
////            int BddName = dvNet.getBddEngine().curBdd;
////            System.out.println("所使用的BDD编号  " + BddName);
//            for (Device device : deviceSet) {
//                device.dvNetRepeat(dvNet);
//            }
//            dvNet.dvNetParseSpace(Device.spacesTmp);
////            dvNet.srcDvNetParseAllSpace(Device.spacesTmp);
//            });
//        }
//        threadPool.awaitAllTaskFinished();
//    }
//
////    @Override
//    public void start() {
//        for(Map.Entry<Integer, DVNet> entry : dvNetMap.entrySet()) {
//            threadPool.execute(() -> {
//            DVNet dvNet = entry.getValue();
//            Set<Device> deviceSet = dvNet.getDevices();
//                // dvnet读取device的规则
////                for (Device device : deviceSet) {
////                    device.dvNetRepeat(dvNet);
////                    device.dvNetParseSpace(device.spacesTmp, dvNet);
////                }
//                // dvnet初始化结点, 指定验证意图
//                dvNet.initNode();
//                System.out.println("netIndex  " + dvNet.netIndex + "完成初始化" );
//                // 开始验证
//                Event rootEvent = Event.getRootEvent("Start");
//                dvNet.startCount(rootEvent);
//            });
//        }
//        threadPool.awaitAllTaskFinished();
//    }
//
////    @Override
////    public void start() {
////        devices.values().forEach(Device::initNode);
////        if (Configuration.getConfiguration().isUseTransformation()) {
////            devices.values().forEach(Device::startSubscribe);
////        }
////        Event rootEvent = Event.getRootEvent("Start");
////        devices.values().forEach(device -> device.startCount(rootEvent));
////        awaitFinished();
////    }
//
//
//    private void initializeDevice() {
//        long readRuleTime = 0;
//        long readSpaceTime = 0;
//        for (Map.Entry<String, Device> entry : devices.entrySet()) {
//            String name = entry.getKey();
//            Device device = entry.getValue();
//            long s_readRuleTime = System.currentTimeMillis();
//            device.bddEngine = srcBdd;
//            device.readRulesFile(Configuration.getConfiguration().getDeviceRuleFile(name));
//            long s_readSpaceTime = System.currentTimeMillis();
//            device.readSpaceFile(Configuration.getConfiguration().getSpaceFile());
//            long e_readRuleTime = System.currentTimeMillis();
//            readRuleTime += (s_readSpaceTime - s_readRuleTime);
//            readSpaceTime += (e_readRuleTime - s_readSpaceTime);
//        }
////        device.dvNetParseSpace(device.spacesTmp, dvNet);
//
//        writeTime(readRuleTime, "./result/readRuleTime.csv");
//        writeTime(readSpaceTime, "./result/readSpaceTime.csv");
//        System.out.println("读取规则的总数:  " + Device.ruleCnt);
//    }
//
//    private void initializeDevice_pall() {
//        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_READ_SIZE);
//
//        for (Map.Entry<String, Device> entry : devices.entrySet()) {
//            String name = entry.getKey();
//            Device device = entry.getValue();
//
//            executorService.execute(() -> {
//                device.readRulesFile(Configuration.getConfiguration().getDeviceRuleFile(name));
//                device.readSpaceFile(Configuration.getConfiguration().getSpaceFile());
//            });
//        }
//
//        // 等待所有任务完成
//        executorService.shutdown();
//        try {
//            executorService.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    // public void addCopyNode() {
//    //     devices.clear();
//    //     for (String deviceName : network.devicePorts.keySet()) {
//    //         // Device d = new Device(deviceName, network, this, threadPool);
//    //         Device d = devices.get(deviceName);
//    //         for(int copyIndex = 1; copyIndex < d.getSpaceMapSize(); copyIndex++){
//    //             for (Map.Entry<Integer, VNode> entry : network.nodes.get(deviceName).entrySet()) {
//    //                 int index = entry.getKey();
//    //                 VNode vn = entry.getValue();
//    //                 if(vn.isEnd){
//
//    //                 }
//    //                 Collection<NodePointer> nextSet = new LinkedList<>();
//    //                 Collection<NodePointer> prevSet = new LinkedList<>();
//    //                 for (VNode next : vn.next) {
//    //                     String nextDevice = next.device;
//    //                     for (DevicePort dp : network.devicePorts.get(deviceName).get(nextDevice)) {
//    //                         nextSet.add(new NodePointer(dp.getPortName(), next.index));
//    //                     }
//    //                 }
//    //                 for (VNode prev : vn.prev) {
//    //                     String prevDevice = prev.device;
//    //                     for (DevicePort dp : network.devicePorts.get(deviceName).get(prevDevice)) {
//    //                         prevSet.add(new NodePointer(dp.getPortName(), prev.index));
//    //                     }
//    //                 }
//    //                 d.addNode(index, prevSet, nextSet, vn.isEnd, vn.invariant);
//    //             }
//    //         }
//
//    //         // devices.put(deviceName, d);
//    //     }
//    //     initializeDevice();
//    // }
//
//    @Override
//    public void awaitFinished(){
//        threadPool.awaitAllTaskFinished(100);
////        for (Device device: devices.values()){
////            device.awaitFinished();
////        }
//    }
//
////    @Override
////    public void sendCountFromNet(Context ctx, DevicePort sendPort) {
////        ctx.setSendPort(sendPort);
////        transfer(ctx, sendPort);
////    }
//
//
//    @Override
//    public void sendCount(Context ctx, DevicePort sendPort, BDDEngine bddEngine) {
//        ctx.setSendPort(sendPort);
//        transfer(ctx, sendPort);
//    }
//
//    private void transfer(Context old_ctx, DevicePort sendPort){
//            DevicePort dst = network.topology.get(sendPort);
//            Device d = devices.get(dst.getDeviceName());
//            Context ctx = new Context();
//            ctx.setCib(old_ctx.getCib());
//            ctx.setTaskID(old_ctx.getTaskID());
//            ctx.setPrevEvent(old_ctx.getPrevEvent());
//            ctx.setSendPort(sendPort);
//            d.receiveCount(ctx, dst);
//    }
//
//    private synchronized void saveBase64(DevicePort recPort, ByteArrayInputStream bais){
//        String base64encodedString = Utility.getBase64FromInputStream(bais);
//        if(saveBW == null) initSaveBW();
//
//        BufferedWriter bw = saveBW.get(recPort.getDeviceName());
//        if(bw == null) {
//            try {
//                bw = new BufferedWriter(new FileWriter(Configuration.getConfiguration().getTracePath() + "/" + recPort.getDeviceName()));
//                saveBW.put(recPort.getDeviceName(), bw);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        try {
//            long time = System.nanoTime();
//            bw.write(time + " " + recPort.getPortName() + " " + base64encodedString + "\n");
//            bw.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public synchronized void initSaveBW(){
//        if(saveBW == null){
//            saveBW = new Hashtable<>();
//            try {
//                File _f = new File(Configuration.getConfiguration().getTracePath());
//                if(!_f.exists()) _f.mkdirs();
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public long getInitTime(){
//        long res = 0;
//        for (Device d: devices.values()){
//            if (d.getInitTime() > res){
//                res = d.getInitTime();
//            }
//        }
//        return res;
//    }
//
//    @Override
//    public void close(){
//        devices.values().forEach(Device::close);
//        threadPool.shutdownNow();
//    }
//
//
//}
