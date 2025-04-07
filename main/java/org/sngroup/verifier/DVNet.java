package org.sngroup.verifier;

import org.sngroup.util.*;
import org.sngroup.util.CopyHelper.*;

import java.util.*;

public class DVNet {
    public int netIndex;

    public Map<String, Node> nodesTable;

    public BDDEngine bddEngine;

    private Node dstNode;

    public int packetSpace;

    // 设备规则命中映射
    public Map<String, Map<Rule, Integer>> deviceRuleHit;
    // 设备规则匹配映射
    public Map<String, Map<Rule, Integer>> deviceRuleMatch;
    // 设备端口谓词映射
    public Map<String, Map<ForwardAction, Integer>> devicePortPredicate;

    // 设备Lec集合
    public Map<String, HashSet<Lec>> deviceLecs;

    // 静态设备包空间映射
    static public Map<String, Integer> devicePacketSpace;
    // 设备规则黑名单映射
    public Map<String, Map<Rule, List<Integer>>> deviceRuleBlacklist;

    // 多线程锁对象，用于同步访问共享资源
    private final Object lock = new Object();

    public DVNet(){
        init();
    }

    public DVNet(int netIndex){
        init();
        this.bddEngine = new BDDEngine();
        this.netIndex = netIndex;
    }

    public DVNet(int netIndex, BDDEngine srcBdd){
        init();
        this.bddEngine = srcBdd;
        this.netIndex = netIndex;
    }

    // 确保设备存在于映射中
    public void putDeviceIfAbsent(String name){
        synchronized (lock) {
            if(!deviceRuleHit.containsKey(name)){
                this.deviceRuleHit.put(name, new HashMap<>());
                this.deviceRuleMatch.put(name, new HashMap<>());
                this.devicePortPredicate.put(name, new HashMap<>());
                this.deviceLecs.put(name, new HashSet<>());
                this.deviceRuleBlacklist.put(name, new HashMap<>());
            }
        }
    }

    // 存储设备规则命中
    public void putDeviceRuleHit(String deviceName, Rule rule, int hit){
        synchronized (deviceRuleHit) {
            deviceRuleHit.get(deviceName).put(rule, hit);
        }
    }

    // 存储设备规则匹配
    public void putDeviceRuleMatch(String deviceName, Rule rule, int match){
        synchronized (deviceRuleMatch) {
            deviceRuleMatch.get(deviceName).put(rule, match);
        }
    }

    // 获取设备规则命中
    public int getDeviceRuleHit(String deviceName, Rule rule){
        return this.deviceRuleHit.get(deviceName).get(rule);
    }

    // 获取设备规则匹配
    public int getDeviceRuleMatch(String deviceName, Rule rule){
        return this.deviceRuleMatch.get(deviceName).get(rule);
    }

    // 获取设备Lec集合
    public HashSet<Lec> getDeviceLecs(String deviceName){
        return this.deviceLecs.get(deviceName);
    }

    // 设置Trie和黑名单
    public void setTrieAndBlacklist(String deviceName, List<Rule> rules){
        Trie trie = new Trie();
        trie.addAndGetAllOverlappingAndAddToBlacklist(rules, this, deviceName);
        trie = null; // 帮助GC回收
    }

    // 添加设备规则黑名单
    public void putDeviceRuleBlacklist(String deviceName, Rule rule, Rule blackRule){
        synchronized (deviceRuleBlacklist) {
            if(!deviceRuleBlacklist.get(deviceName).containsKey(rule)){
                this.deviceRuleBlacklist.get(deviceName).put(rule, new ArrayList<>());
            }
            int black = this.deviceRuleMatch.get(deviceName).get(blackRule);
            this.deviceRuleBlacklist.get(deviceName).get(rule).add(black);
        }
    }

    // 获取设备规则黑名单
    public List<Integer> getDeviceRuleBlacklist(String deviceName, Rule rule){
        synchronized (deviceRuleBlacklist) {
            if(!deviceRuleBlacklist.get(deviceName).containsKey(rule)){
                this.deviceRuleBlacklist.get(deviceName).put(rule, new ArrayList<>());
            }
            return this.deviceRuleBlacklist.get(deviceName).get(rule);
        }
    }

    // 解析所有IP空间
    public void srcDvNetParseAllSpace(Map<String, List<IPPrefix>> spaces){
        BDDEngine bddEngine = this.getBddEngine();
        synchronized (devicePacketSpace) {
            for(Map.Entry<String, List<IPPrefix>> entry : spaces.entrySet()) {
                String dstDevice = entry.getKey();
                List<IPPrefix> ipPrefixList = spaces.get(dstDevice);
                int s = bddEngine.encodeDstIPPrefixList(ipPrefixList);
                devicePacketSpace.put(dstDevice, s);
            }
        }
    }

    // 解析所有IPv6空间
    public void srcDvNetParseAllSpaceIPV6(Map<String, List<IPPrefixIPV6>> spacesIPV6){
        BDDEngine bddEngine = this.getBddEngine();
        synchronized (devicePacketSpace) {
            for(Map.Entry<String, List<IPPrefixIPV6>> entry : spacesIPV6.entrySet()) {
                String dstDevice = entry.getKey();
                List<IPPrefixIPV6> ipPrefixList = spacesIPV6.get(dstDevice);
                int s = bddEngine.encodeDstIPPrefixListIPV6(ipPrefixList);
                devicePacketSpace.put(dstDevice, s);
            }
        }
    }

    // 复制BDD引擎
    public void copyBdd(BDDEngine srcBdd, String copyType) throws Exception {
        BDDEngine bddCopy = null;
        if(Objects.equals(copyType, "Reflect")){
            ReflectDeepCopy copyHelper = new ReflectDeepCopy();
            bddCopy = (BDDEngine) copyHelper.deepCopy(srcBdd);
        }
        this.bddEngine = bddCopy;
        for(Node node : nodesTable.values()){
            assert this.bddEngine != null;
            node.setBdd(this.bddEngine);
        }
    }

    // 获取BDD引擎
    public BDDEngine getBddEngine(){
        return this.bddEngine;
    }

    // 设置包空间
    public void setPacketSpace(int s) {
        this.packetSpace = s;
    }

    // 设置目标节点
    public void setDstNode(Node node){
        this.dstNode = node;
    }

    // 获取目标节点
    public Node getDstNode() {
        return this.dstNode;
    }

    // 初始化映射
    public void init(){
        this.deviceRuleHit = new HashMap<>();
        this.deviceRuleMatch = new HashMap<>();
        this.devicePortPredicate = new HashMap<>();
        this.deviceLecs = new HashMap<>();
        this.deviceRuleBlacklist = new HashMap<>();
        devicePacketSpace = new HashMap<>();
    }
}