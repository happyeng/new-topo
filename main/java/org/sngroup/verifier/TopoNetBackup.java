//package org.sngroup.verifier;
//
//import org.sngroup.util.*;
//
//import org.sngroup.util.CopyHelper.*;
//
//import java.util.*;
//
//import org.apache.commons.lang3.SerializationUtils;
//
//public class TopoNet {
//    public int netIndex;
//
//    public int topoCnt;
//
//    public Set<Device> srcDeivces;
//
//    Map<String, Device> devices;
//
//
//    public Set<Node> srcNodes;
//    public Device dstDevice;
//
//    public Network network;
//
//    public Invariant invariant;
//
//    protected List<Node> nodes;
//
//
//    protected BDDEngine bddEngine;
//
//    protected Node dstNode;
//
////    protected List<Node> srcNodes;
//
//    public int packetSpace;
//
//    public Map<String, Trie> trieTable;
//
//    static public Set<Device> edgeDevices = new HashSet<>();
//
//    static public Map<String, Set<DevicePort>> devicePortsTopo;
//
//
//    // 先分开管理, 验证并行度, 之后再行合并
//    // device rule hit
//    public Map<String, Map<Rule, Integer>> deviceRuleHit;
//    public Map<String, Map<Rule, Integer>> deviceRuleMatch;
//    public Map<String, Map<Rule, Integer>> deviceRuleLecIndex;
//    public Map<String, Map<ForwardAction, Integer>> devicePortPredicate;
//    public Map<String, Map<ForwardAction, Integer>> devicePortIndex;
//    public Map<String, List<Lec>> deviceLecs;
//
//    static public Map<String, Integer> devicePacketSpace;
//    public Map<String, Map<Rule, List<Integer>>> deviceRuleBlacklist;
//
//
//    public TopoNet(){
//        init();
//    }
//
//
//    public TopoNet(Device dstDevice, int topoCnt){
//        super();
//        init();
//        this.dstDevice = dstDevice;
//        this.topoCnt = topoCnt;
//    }
//
//
//    public void putDeviceIfAbsent(String name){
//        if(!deviceRuleHit.containsKey(name)){
//            this.deviceRuleHit.put(name, new HashMap<>());
//            this.deviceRuleMatch.put(name, new HashMap<>());
//            this.deviceRuleLecIndex.put(name, new HashMap<>());
//            this.devicePortPredicate.put(name, new HashMap<>());
//            this.devicePortIndex.put(name, new HashMap<>());
//            this.deviceLecs.put(name, new ArrayList<>());
//            this.deviceRuleBlacklist.put(name, new HashMap<>());
//        }
//        else return;
//    }
//
//    // put Attribute
//    // get Attribute
//    public void putDeviceRuleHit(String deviceName, Rule rule, int hit){
////        if(!deviceRuleHit.get(deviceName).containsKey(rule))
//        deviceRuleHit.get(deviceName).put(rule, hit);
//    }
//
//    public void putDeviceRuleMatch(String deviceName, Rule rule, int match){
////        if(!deviceRuleMatch.get(deviceName).containsKey(rule))
//        deviceRuleMatch.get(deviceName).put(rule, match);
//    }
//    public void putDeviceRuleLecIndex(String deviceName, Rule rule, int lecIndex){
////        if(!deviceRuleLecIndex.get(deviceName).containsKey(rule))
//        deviceRuleLecIndex.get(deviceName).put(rule, lecIndex);
//    }
//    public void putDevicePortPredicate(String deviceName, ForwardAction forwardAction, int predicate){
////        if(!devicePortPredicate.get(deviceName).containsKey(forwardAction))
//        devicePortPredicate.get(deviceName).put(forwardAction, predicate);
//    }
//    public void putDevicePortIndex(String deviceName,  ForwardAction forwardAction, int index){
////        if(!devicePortIndex.get(deviceName).containsKey(forwardAction))
//        devicePortIndex.get(deviceName).put(forwardAction, index);
//    }
//
//    // get Attribute
//    public int getDeviceRuleHit(String deviceName, Rule rule){
//        return this.deviceRuleHit.get(deviceName).get(rule);
//    }
//
//    public int getDeviceRuleMatch(String deviceName, Rule rule){
//        return this.deviceRuleMatch.get(deviceName).get(rule);
//    }
//    public int getDeviceRuleLecIndex(String deviceName, Rule rule){
//        return this.deviceRuleLecIndex.get(deviceName).get(rule);
//    }
//    public int getDevicePortPredicate(String deviceName, ForwardAction forwardAction){
//        return this.devicePortPredicate.get(deviceName).get(forwardAction);
//    }
//    public int getDevicePortIndex(String deviceName,  ForwardAction forwardAction){
//        return this.devicePortIndex.get(deviceName).get(forwardAction);
//    }
//    public List<Lec> getDeviceLecs(String deviceName){
//        return this.deviceLecs.get(deviceName);
//    }
//
//    public String getDstDeviceName(){return this.dstNode.device.name;}
//
//
//
//    public void setTrieAndBlacklist(String deviceName, List<Rule> rules){
//        Trie trie = new Trie();
//        trie.addAndGetAllOverlappingAndAddToBlacklist(rules, this, deviceName);
//        trie = null;
//    }
//
//    public void putDeviceRuleBlacklist(String deviceName, Rule rule, Rule blackRule){
////        if(!deviceRuleHit.get(deviceName).containsKey(rule))
//        if(!deviceRuleBlacklist.get(deviceName).containsKey(rule)){
//            this.deviceRuleBlacklist.get(deviceName).put(rule, new ArrayList<>());
//        }
//        // int black = getDeviceRuleMatch(deviceName, blackRule);
//        Map<Rule, Integer> tt = this.deviceRuleMatch.get(deviceName);
//        int black = this.deviceRuleMatch.get(deviceName).get(blackRule);
//        this.deviceRuleBlacklist.get(deviceName).get(rule).add(black);
//    }
//
//    public List<Integer> getDeviceRuleBlacklist(String deviceName, Rule rule){
//        if(!deviceRuleBlacklist.get(deviceName).containsKey(rule)){
//            this.deviceRuleBlacklist.get(deviceName).put(rule, new ArrayList<>());
//        }
//        return this.deviceRuleBlacklist.get(deviceName).get(rule);
//    }
//
//
//
////    public void srcDvNetParseAllSpace(Map<String, List<IPPrefix>> spaces, String device){
////        BDDEngine bddEngine = this.getBddEngine();
////        List<IPPrefix> ipPrefixList = spaces.get(device);
////        int s = bddEngine.encodeDstIPPrefixList(ipPrefixList);
////        devicePacketSpace.put(device, s);
////        System.out.println("device  " + device + "转化后得到的s  " + s);
////    }
//
//    public void srcDvNetParseAllSpace(Map<String, List<IPPrefix>> spaces){
//        BDDEngine bddEngine = this.getBddEngine();
//        for(Map.Entry<String, List<IPPrefix>> entry : spaces.entrySet())
//        {
//            String dstDevice = entry.getKey();
//            List<IPPrefix> ipPrefixList = spaces.get(dstDevice);
////            IPPrefix prefixListFirst = ipPrefixList.get(0);
//            int s = bddEngine.encodeDstIPPrefixList(ipPrefixList);
////            int s = bddEngine.encodeDstIPPrefix(prefixListFirst.ip, prefixListFirst.prefix);
//            devicePacketSpace.put(dstDevice, s);
////            if(Objects.equals(dstDevice, this.getDstDeviceName())){
////                this.setPacketSpace(s);
////            }
////            System.out.println("device  " + dstDevice + "转化后得到的s  " + s);
//        }
//
//    }
//
//    public  void  dvNetParseSpace(Map<String, List<IPPrefix>> spaces){
//        // 只需要拿到dvnet中目的device的packet Space 即可
//        BDDEngine bddEngine = this.getBddEngine();
//        Node dstNode = this.getDstNode();
//        String dstDevice = dstNode.getDeviceName();
//        List<IPPrefix> ipPrefixList = spaces.get(dstDevice);
//        int s = bddEngine.encodeDstIPPrefixList(ipPrefixList);
////        System.out.println("目的结点转化S  " + s);
//        this.setPacketSpace(s);
////        spaces.clear();
////        long t1 = System.nanoTime();
////        deviceSpace = new HashMap<>();
////        Node dstNode = dvNet.getDstNode();
////        String dstDevice = dstNode.getDeviceName();
////        for(Map.Entry<String, List<IPPrefix>> entry: spaces.entrySet()){
////            String device = entry.getKey();
////            List<IPPrefix> ipPrefixList = entry.getValue();
////            // add_dfy 只设置第一个packet space
////             int s = bddEngine.encodeDstIPPrefixList(entry.getValue());
////            deviceSpace.put(device, s);
////            if(device == dstDevice) dvNet.setPacketSpace(s);
////        }
////        long t2 = System.nanoTime();
////        buildSpaceTime = t2-t1;
////        spaces.clear();
//    }
//
//    public void copyBdd(BDDEngine srcBdd, String copyType) throws Exception {
//        BDDEngine bddCopy = null;
//        if(Objects.equals(copyType, "Reflect")){
//            ReflectDeepCopy copyHelper = new ReflectDeepCopy();
//            bddCopy = (BDDEngine) copyHelper.deepCopy(srcBdd);
//        }
//        else if(Objects.equals(copyType, "FST")){
//            FSTDeepCopy copyHelper = new FSTDeepCopy();
//            bddCopy = copyHelper.deepCopy(srcBdd);
//        }
//        else if(Objects.equals(copyType, "Kryo")){
//            KryoDeepCopy copyHelper = new KryoDeepCopy();
//            copyHelper.kryoRegister( );
//            bddCopy = copyHelper.deepCopy(srcBdd);
//        }
//        else if(Objects.equals(copyType, "Apache")){  // default to Apache
//            bddCopy = SerializationUtils.clone(srcBdd);
//        }
//        this.bddEngine = bddCopy;
//        for(Node node : nodes){
//            assert this.bddEngine != null;
//            node.setBdd(this.bddEngine);
//        }
//    }
//
//    public void copyLecs(Map<String, List<Lec>> srcLecs){
////         拷贝lec
//        Map<String, List<Lec>> deepCopy = new HashMap<>();
//        for (Map.Entry<String, List<Lec>> entry : srcLecs.entrySet()) {
//            List<Lec> listCopy = new ArrayList<>();
//            for (Lec lec : entry.getValue()) {
//                listCopy.add(new Lec(lec));
//            }
//            deepCopy.put(entry.getKey(), listCopy);
//        }
//        this.deviceLecs = deepCopy;
//        // 共用lec
////        this.deviceLecs = srcLecs;
//    }
//
//    // 与Topo有关操作
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
//        public void initOtherNodes(Device device){
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
//    }
//
//
//    public BDDEngine getBddEngine(){
//        return this.bddEngine;
//    }
//
//    public void setPacketSpace(int s) {this.packetSpace = s;}
//
//    public void setDstNode(Node node){
//        this.dstNode = node;
//    }
//
//    public void addNode(Node node){ this.nodes.add(node);}
//
//    public void addSrcNode(Node node){
//        this.srcNodes.add(node);
//    }
//
//    public Node getDstNode() {return this.dstNode;}
//
//    public List<Node> getNodes(){return this.nodes;}
//
//    public int getPacketSpace(){return this.packetSpace;}
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
//    public void initNode(){
//        for(Node node:nodes){
////            synchronized (this.bddEngine) {
//            node.topoNetStart();
////            }
//        }
//    }
//
////    public void startCount(Event event){
////        System.out.println("topoNET  " + topoCnt + "  终点开始计数");
////        Context c = new Context();
////        c.setTaskID(event.id);
////        c.setPrevEvent(event);
////        this.dstNode.startCount(c);
////    }
//
//
//
//    public void init(){
//        this.srcDeivces = new HashSet<>();
//        this.devices = new HashMap<>();
//        this.srcNodes = new HashSet<>();
//        this.nodes = new ArrayList<>();
//        this.deviceRuleHit = new HashMap<>();
//        this.deviceRuleMatch = new HashMap<>();
//        this.deviceRuleLecIndex = new HashMap<>();
//        this.devicePortPredicate = new HashMap<>();
//        this.devicePortIndex = new HashMap<>();
//        this.deviceLecs = new HashMap<>();
//        this.trieTable = new HashMap<>();
//        this.deviceRuleBlacklist = new HashMap<>();
//        devicePacketSpace = new HashMap<>();
//    }
//
//
//
//}