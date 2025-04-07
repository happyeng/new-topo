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
//import org.sngroup.Configuration;
//import org.sngroup.util.*;
//import org.sngroup.verifier.BDDEngine;
//import org.sngroup.verifier.Context;
//import org.sngroup.verifier.Device;
//import org.sngroup.verifier.serialization.Serialization;
//import org.sngroup.verifier.serialization.proto.ProtobufSerialization;
//
//import java.io.*;
//import java.util.*;
//import java.util.Collection;
//import java.util.Hashtable;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.Map;
//import java.util.Queue;
//import java.util.HashMap;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class DebugFattreeRunner extends Runner {
//    public ThreadPool threadPool;
//    // private final Serialization srl;
//    Map<String, Device> devices;
//
//    Queue<MessageToken> edgeMessageQueue;
//    Queue<MessageToken> coreMessageQueue;
//    Queue<MessageToken> aggregationMessageQueue;
//    int npod;
//    int sourceIndex;
//    int aggregationIndex;
//    int coreIndex;
//    int edgeIndex;
//
//    boolean onlySource;
//    boolean selectAggregation;
//    boolean selectCore;
//    boolean selectEdge;
//    boolean rswSave;
//
//    Map<String, BufferedWriter> saveBW;
//    public DebugFattreeRunner(int kvalue){
//        super();
//
//        // srl = new ProtobufSerialization();
//        devices = new Hashtable<>();
//
//        aggregationMessageQueue = new LinkedBlockingQueue<>();
//        coreMessageQueue = new LinkedBlockingQueue<>();
//        edgeMessageQueue = new LinkedBlockingQueue<>();
//        this.npod = kvalue;
//        onlySource = true;
//        selectAggregation = false;
//        selectCore = false;
//        selectEdge = false;
//        rswSave = false;
//    }
//
//    @Override
//    public void build() {
//        threadPool = ThreadPool.FixedThreadPool(Configuration.getConfiguration().getThreadPoolSize());
//
//        // DebugFattreeRunner n = new DebugFattreeRunner(k);
//        //// create links
//        devices.clear();
//        int k = this.npod;
//        for (int iPod = 0; iPod < k; iPod++) {
//            for (int iFsw = 0; iFsw < k / 2; iFsw++) {
//                String fsw = "fsw-" + iPod + "-" + iFsw;
//                // down links
//                for (int iRsw = 0; iRsw < k / 2; iRsw++) {
//                    String rsw = "rsw-" + iPod + "-" + iRsw;
//                    this.network.addTopology(fsw, fsw + ">" + rsw, rsw, rsw + ">" + fsw);
//                }
//                // up links
//                for (int iSsw = iFsw * k / 2; iSsw < iFsw * k / 2 + k / 2; iSsw++) {
//                    String ssw = "ssw-" + iSsw;
//                    this.network.addTopology(fsw, fsw + ">" + ssw, ssw, ssw + ">" + fsw);
//                }
//            }
//        }
//
//        this.addCoreSwitch();
//        this.addAggregationSwitch();
//        this.addEdgeSwitch();
//
//        //// \create links
//        // return n;
//    }
//
//    public void addEdgeSwitch(){
//        addEdgeSwitch(true);
//    }
//
//    public void addEdgeSwitch(boolean addInitTime){
//        for (int iPod = 0; iPod < npod; iPod++) {
//            for (int iRsw = 0; iRsw < npod / 2; iRsw++) {
//                if(selectEdge && edgeIndex!=iRsw) continue;
//                String rsw = "rsw-" + iPod + "-" + iRsw;
//                Device device = new Device(rsw, this.network, this, threadPool);
//                Map<String, List<IPPrefix>> spaces = new HashMap<>();
//
//                devices.put(rsw, device);
//
//                List<Rule> rules = new LinkedList<>();
//                for (int jPod = 0; jPod < npod; jPod++) {
//                    for (int jRsw = 0; jRsw < npod / 2; jRsw++) {
//                        String dstrsw = "rsw-" + jPod + "-" + jRsw;
//                        long dstip = ((long) jPod << 24) + ((long) (jRsw + 1) << 16);
//                        if (dstrsw.equals(rsw)) {
//
//
//                            // device.addSpace(rsw, dstip, 16);
//                            rules.add(new Rule(dstip, 16, rsw + ">h-" + jPod + "-" + jRsw));
//                        } else {
//                            String dstfsw = "fsw-" + iPod + "-" + (npod / 2 * jPod + jRsw) % (npod / 2);
//                            rules.add(new Rule(dstip, 16, rsw + ">" + dstfsw));
//                        }
//                        IPPrefix space = new IPPrefix(dstip, 16);
//                        spaces.putIfAbsent(dstrsw, new LinkedList<>());
//                        spaces.get(dstrsw).add(space);
//                    }
//                }
//                device.getFattreeSpace(spaces);
//                device.readRules(rules);
//            }
//        }
//        addEdgeNode();
//    }
//
//    public void addEdgeNode(Device device, int pod, int rsw){
//        // List<NodePointer> next = new LinkedList<>();
//        // List<NodePointer> prev = new LinkedList<>();
//        // int i = pod * (npod / 2) + rsw;
//        // for (int jRsw = 0; jRsw < npod/2; jRsw++) {
//        //     String dstfsw = "fsw-" + pod + "-" + jRsw;
//        //     next.add(new NodePointer(device.name + ">" + dstfsw, rsw));
//        //     for (int index = 0; index <= npod/2; index++) {
//        //         if(index == rsw) continue;
//        //         prev.add(new NodePointer(device.name + ">" + dstfsw, index));
//        //     }
//        // }
//
//        for(int i = 0; i < npod*npod/2; i++){
//            for(int j = 0; j < npod*npod/2; j++){
//                if(i == j) continue;
//                int cpindex = i * npod*npod/2 + j;
//                int srcpod = i/(npod/2);
//                int srcindex = (i - (i/(npod/2))*(npod/2));
//                int dstpod = j/(npod/2);
//                int dstindex = (j - (j/(npod/2))*(npod/2));
//
//
//                String match = "exists >= 1";
//                String rsw1 = "rsw-" + srcpod + "-" + srcindex;
//                String rsw2 = "rsw-" + dstpod + "-" + dstindex;
//                String path = rsw1 + ".*" + rsw2;
//                String packeSpace = rsw2;
//                Invariant invariant = new Invariant(packeSpace, match, path);
//
//                if(srcpod == pod){
//                    List<NodePointer> next = new LinkedList<>();
//                    // prev.add(new NodePointer(device.name + ">" + "rsw-" + srcpod + "-" + srcindex, cpindex));
//                    for(int m = 0; m < npod/2; m++){
//                        String dstfsw = "fsw-" + pod + "-" + m;
//                        next.add(new NodePointer(device.name + ">" + dstfsw, cpindex));
//                    }
//
//                    device.addNode(cpindex, new LinkedList<>(), next,false, invariant);
//
//                }else if(dstpod == pod){
//                    List<NodePointer> prev = new LinkedList<>();
//                    for(int m = 0; m < npod/2; m++){
//                        String dstfsw = "fsw-" + pod + "-" + m;
//                        prev.add(new NodePointer(device.name + ">" + dstfsw, cpindex));
//                    }
//
//                    device.addNode(cpindex, prev, new LinkedList<>(),true, invariant);
//
//
//                }else{
//                    continue;
//                }
//            }
//        }
//    }
//
//    public void addEdgeNode(){
//        for (int iPod = 0; iPod < npod; iPod++) {
//            for (int iRsw = 0; iRsw < npod / 2; iRsw++) {
//                String rsw = "rsw-" + iPod + "-" + iRsw;
//                Device device = devices.get(rsw);
//                addEdgeNode(device, iPod, iRsw);
//            }
//        }
//    }
//
//    public void addAggregationSwitch(){
//        for (int iPod = 0; iPod < npod; iPod++){
//            for (int iFsw = 0; iFsw < npod/2; iFsw++) {
//                String fsw = "fsw-" + iPod + "-" + iFsw;
//                Device device = new Device(fsw, this.network, this, threadPool);
//                Map<String, List<IPPrefix>> spaces = new HashMap<>();
//
//                devices.put(fsw, device);
//                List<Rule> rules = new LinkedList<>();
//                for (int jPod = 0; jPod < npod; jPod++) {
//                    for (int jRsw = 0; jRsw < npod/2; jRsw++) {
//                        String dstrsw = "rsw-" + jPod + "-" + jRsw;
//
//                        long dstip = ((long) jPod << 24) + ((long) (jRsw + 1) << 16);
//
//                        IPPrefix space = new IPPrefix(dstip, 16);
//                        spaces.putIfAbsent(dstrsw, new LinkedList<>());
//                        spaces.get(dstrsw).add(space);
//
//                        if (jPod == iPod) { // intra pod
//                            rules.add(new Rule(dstip, 16, fsw + ">" + dstrsw));
//                        } else {
//                            String dstssw = "ssw-" + (iFsw * (npod/2) + ((npod/2) * jPod + jRsw) % (npod/2));
//                            rules.add(new Rule(dstip, 16, fsw + ">" + dstssw));
//                        }
//                    }
//                }
//                device.getFattreeSpace(spaces);
//                device.readRules(rules);
//            }
//        }
//        addAggregationNode();
//    }
//
//    public void addAggregationNode(Device device, int iPod, int fsw){
//
//        for(int i = 0; i < npod*npod/2; i++){
//            for(int j = 0; j < npod*npod/2; j++){
//                if(i == j) continue;
//                int cpindex = i * npod*npod/2 + j;
//                int srcpod = i/(npod/2);
//                int srcindex = (i - (i/(npod/2))*(npod/2));
//                int dstpod = j/(npod/2);
//                int dstindex = (j - (j/(npod/2))*(npod/2));
//
//
//                String match = "exists >= 1";
//                String rsw1 = "rsw-" + srcpod + "-" + srcindex;
//                String rsw2 = "rsw-" + dstpod + "-" + dstindex;
//                String path = rsw1 + ".*" + rsw2;
//                String packeSpace = rsw2;
//                Invariant invariant = new Invariant(packeSpace, match, path);
//
//                if(srcpod == iPod){
//                    List<NodePointer> next = new LinkedList<>();
//                    List<NodePointer> prev = new LinkedList<>();
//                    prev.add(new NodePointer(device.name + ">" + "rsw-" + srcpod + "-" + srcindex, cpindex));
//                    for(int m = 0; m < npod/2; m++){
//                        next.add(new NodePointer(device.name + ">" + "ssw-" + (m + fsw*(npod/2)), cpindex));
//                    }
//
//                    device.addNode(cpindex, prev, next,false, invariant);
//
//                }else if(dstpod == iPod){
//                    List<NodePointer> next = new LinkedList<>();
//                    List<NodePointer> prev = new LinkedList<>();
//                    next.add(new NodePointer(device.name + ">" + "rsw-" + dstpod + "-" + dstindex, cpindex));
//                    for(int m = 0; m < npod/2; m++){
//                        prev.add(new NodePointer(device.name + ">" + "ssw-" + (m + fsw*(npod/2)), cpindex));
//                        device.addNode(cpindex*(m+1), prev, next,false, invariant);
//                    }
//
//
//                }else{
//                    continue;
//                }
//            }
//        }
//
//    }
//
//    public void addAggregationNode(){
//        for (int iPod = 0; iPod < npod; iPod++) {
//            for (int iFsw = 0; iFsw < npod / 2; iFsw++) {
//                String fsw = "fsw-" + iPod + "-" + iFsw;
//                Device device = devices.get(fsw);
//                if(device != null)
//                    addAggregationNode(device, iPod, iFsw);
//            }
//        }
//    }
//
//    public void addCoreSwitch(){
//        for (int iSsw = 0; iSsw < npod*npod/4; iSsw++) {
//            if(selectCore && iSsw!=coreIndex) continue;
//            String ssw = "ssw-" + iSsw;
//            Device device = new Device(ssw, this.network, this, threadPool);
//            Map<String, List<IPPrefix>> spaces = new HashMap<>();
//
//
//
//            devices.put(ssw, device);
//            List<Rule> rules = new LinkedList<>();
//            for (int k = 0; k < npod; k++) {
//                for (int l = 0; l < npod/2; l++) {
//                    long dstip = ((long) k << 24) + ((long) (l + 1) << 16);
//                    String dstfsw = "fsw-" + k + "-" + (iSsw / (npod/2));
//                    rules.add(new Rule(dstip, 16, ssw + ">" + dstfsw));
//
//                    String dstrsw = "rsw-" +k + "-" + l;
//
//                    IPPrefix space = new IPPrefix(dstip, 16);
//                    spaces.putIfAbsent(dstrsw, new LinkedList<>());
//                    spaces.get(dstrsw).add(space);
//                }
//            }
//            device.getFattreeSpace(spaces);
//            device.readRules(rules);
//        }
//        addCoreNode();
//    }
//
//
//    public void addCoreNode(Device device, int iSsw) {
//        int coreindex = (iSsw / (npod / 2));
//        for(int i = 0; i < npod*npod/2; i++){
//            for(int j = 0; j < npod*npod/2; j++){
//                if(i == j) continue;
//                int cpindex = i * npod*npod/2 + j;
//                int srcpod = i/(npod/2);
//                int srcindex = (i - (i/(npod/2))*(npod/2));
//                int dstpod = j/(npod/2);
//                int dstindex = (j - (j/(npod/2))*(npod/2));
//
//
//                String match = "exists >= 1";
//                String rsw1 = "rsw-" + srcpod + "-" + srcindex;
//                String rsw2 = "rsw-" + dstpod + "-" + dstindex;
//                String path = rsw1 + ".*" + rsw2;
//                String packeSpace = rsw2;
//                Invariant invariant = new Invariant(packeSpace, match, path);
//
//                List<NodePointer> next = new LinkedList<>();
//                List<NodePointer> prev = new LinkedList<>();
//                prev.add(new NodePointer(device.name + ">" + "fsw-" + i/(npod/2) + "-" + coreindex, cpindex));
//                next.add(new NodePointer(device.name + ">" + "fsw-" + j/(npod/2) + "-" + coreindex, cpindex*(coreindex+1)));
//
//                device.addNode(cpindex, prev, next,false, invariant);
//            }
//        }
//    }
//
//    public void addCoreNode(){
//        for (int iSsw = 0; iSsw < npod*npod/4; iSsw++) {
//                String ssw = "ssw-" + iSsw;
//                Device device = devices.get(ssw);
//                if(device != null)
//                    addCoreNode(device, iSsw);
//        }
//    }
//
//    // public void removeNode(){
//    //     for(Device device: devices.values()){
//    //         device.nodes.clear();
//    //         device.dstNodes.clear();
//    //     }
//    //     gc();
//    // }
//
//
//
//    public ThreadPool getThreadPool(){
//        return threadPool;
//    }
//    public Device getDevice(String name){
//        return devices.get(name);
//    }
//
//
//
//
//    @Override
//    public void awaitFinished(){
//        threadPool.awaitAllTaskFinished(100);
////        for (Device device: devices.values()){
////            device.awaitFinished();
////        }
//    }
//
//    @Override
//    public void sendCount(Context ctx, DevicePort sendPort, BDDEngine bddEngine) {
//        ctx.setSendPort(sendPort);
//        transfer(ctx, sendPort);
//    }
//
//    private void transfer(Context old_ctx, DevicePort sendPort){
//        threadPool.execute(()-> {
//            DevicePort dst = network.topology.get(sendPort);
//            Device d = devices.get(dst.getDeviceName());
//            Context ctx = new Context();
//            ctx.setCib(old_ctx.getCib());
//            ctx.setTaskID(old_ctx.getTaskID());
//            ctx.setPrevEvent(old_ctx.getPrevEvent());
//            ctx.setSendPort(sendPort);
//            d.receiveCount(ctx, dst);
//        });
//    }
//
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
//    @Override
//    public void start() {
//        devices.values().forEach(Device::initNode);
//        if (Configuration.getConfiguration().isUseTransformation()) {
//            devices.values().forEach(Device::startSubscribe);
//        }
//        Event rootEvent = Event.getRootEvent("Start");
//        devices.values().forEach(device -> device.startCount(rootEvent));
//        awaitFinished();
//
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
//    class MessageToken{
//        OutputStream os;
//        DevicePort dst;
//        MessageToken(OutputStream os, DevicePort dst){
//            this.os = os;
//            this.dst = dst;
//        }
//    }
//
//
//}
