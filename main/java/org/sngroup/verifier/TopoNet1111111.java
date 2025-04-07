//package org.sngroup.verifier;
//
//import org.apache.commons.lang3.ObjectUtils;
//import org.sngroup.util.*;
//
//import java.util.*;
//
//public class TopoNet extends DVNet {
//
//    static public Set<Device> edgeDevices = new HashSet<>();
//
//    static public Map<String, Set<DevicePort>> devicePortsTopo;
//
//    public int topoCnt;
//
//    public Set<Device> srcDeivces;
//
//
//    public Set<Node> srcNodes;
//    public Device dstDevice;
//
//    public Network network;
//
//    public Invariant invariant;
//
//    Map<String, Device> devices;
//
//
//
//    public TopoNet() {
//        super();
//        init();
//
//    }
//
//    public TopoNet(Device dstDevice, int topoCnt){
//        super();
//        init();
//        this.dstDevice = dstDevice;
//        this.topoCnt = topoCnt;
//    }
//
//    public void setNetwork(Network network){
//        this.network = network;
//    }
//
//    public void setDevices(Map<String, Device> devices){
//        this.devices = devices;
//    }
//
//    public void setInvariant(String packetSpace, String match, String path){
//        this.invariant = new Invariant(packetSpace, match, path);
//    }
//
//    public synchronized void initNode(){
//        assert nodes = ObjectUtils.NULL;
//        for(Node node:nodes){
//            synchronized (this.bddEngine) {
//            node.topoNetStart();
//            }
//        }
//    }
//
//    public static void  transformDevicePorts(Map<String, Map<String, Set<DevicePort>>> devicePortsOriginal){
//        // 将 devicePortsOriginal 转换为 devicePortsNew 格式
//        Map<String, Set<DevicePort>> devicePortsNew = new HashMap<>();
//        for (Map.Entry<String, Map<String, Set<DevicePort>>> entry : devicePortsOriginal.entrySet()) {
//            String key = entry.getKey();
//            Map<String, Set<DevicePort>> innerMap = entry.getValue();
//            // 创建一个新的 Set 来存储与外部相连的端口
//            Set<DevicePort> connectedPorts = new HashSet<>();
//            for (Set<DevicePort> portSet : innerMap.values()) {
//                // 将所有与外部相连的端口添加到 connectedPorts 中
//                connectedPorts.addAll(portSet);
//            }
//            devicePortsNew.put(key, connectedPorts);
//        }
//        devicePortsTopo = devicePortsNew;
//    }
//
//
//    public void initDstNode(){
////        Collection<NodePointer> nextSet = new LinkedList<>();
////        Collection<NodePointer> prevSet = new LinkedList<>();
////        for(Map.Entry<String, Set<DevicePort>> entry : network.devicePorts.get(dstDevice.name).entrySet()){
////            for (DevicePort dp : entry.getValue()) {
////                nextSet.add(new NodePointer(dp.getPortName(), topoCnt));
////            }
////        }
//        Node node = new Node(dstDevice, topoCnt, invariant);
////        for(NodePointer np: nextSet) {
////            node.addNext(np);
////        }
//        node.setTopoNet(this);
//        this.nodes.add(node);
//        this.dstNode = node;
//        node.isDestination = true;
//        synchronized (dstDevice) {
//            dstDevice.addTopoNode(topoCnt, node);
//        }
//    }
//
//    public void initSrcNodes(Device srcDevice) {
//        // 除dstdevice外的所有edgeDevice都为srcDevice
//        srcDeivces.add(srcDevice);
//        // 只需考虑入度
//        Collection<NodePointer> nextSet = new LinkedList<>();
//        for (Map.Entry<String, Set<DevicePort>> entry : network.devicePorts.get(srcDevice.name).entrySet()) {
//            for (DevicePort dp : entry.getValue()) {
//                nextSet.add(new NodePointer(dp.getPortName(), topoCnt));
//            }
//        }
//        Node node = new Node(srcDevice, topoCnt, invariant);
//        for (NodePointer np : nextSet) {
//            node.addNext(np);
//        }
//        this.nodes.add(node);
//        this.srcNodes.add(node);
//        node.setTopoNet(this);
//        synchronized (srcDevice) {
//            srcDevice.addTopoNode(topoCnt, node);
//        }
//    }
//
//    public void initOtherNodes(Device device){
//        // 入度和出度皆需要考虑
//        Collection<NodePointer> nextSet = new LinkedList<>();
//        for(Map.Entry<String, Set<DevicePort>> entry : network.devicePorts.get(device.name).entrySet()){
//            for (DevicePort dp : entry.getValue()) {
//                nextSet.add(new NodePointer(dp.getPortName(), topoCnt));
//            }
//        }
//        Node node = new Node(device, topoCnt, invariant);
//        for(NodePointer np: nextSet) {
//            node.addNext(np);
//        }
//        node.setTopoNet(this);
//        this.nodes.add(node);
//        synchronized (device) {
//            device.addTopoNode(topoCnt, node);
//        }
//    }
//
//    public void startCount(Event event){
//        Context c = new Context();
//        c.topoId = this.topoCnt;
//        c.setTaskID(event.id);
//        c.setPrevEvent(event);
//        System.out.println("toponet  " + topoCnt + "  终点开始计数  " + dstNode.nodeName);
//        this.dstNode.startCountByTopo(c);
//        System.out.println("topoNet  " + topoCnt + " 结束递归, 统计结果");
//        for(Node node : srcNodes){
//            node.showResult();
//        }
//
//    }
//
//
//
//    public void init(){
//        srcDeivces = new HashSet<>();
//        srcNodes = new HashSet<>();
//        this.nodes = new ArrayList<>();
//
//    }
//
//}
