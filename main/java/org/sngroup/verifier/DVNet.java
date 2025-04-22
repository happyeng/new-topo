package org.sngroup.verifier;

import org.sngroup.util.*;

import org.sngroup.util.CopyHelper.*;

import java.util.*;

import org.apache.commons.lang3.SerializationUtils;

public class DVNet {
    public int netIndex;

//    public List<Node> nodes;

    public Map<String, Node> nodesTable;


    public BDDEngine bddEngine;

    private Node dstNode;

    public int packetSpace;

    // 先分开管理, 验证并行度, 之后再行合并
    // device rule hit
    public Map<String, Map<Rule, Integer>> deviceRuleHit;
    public Map<String, Map<Rule, Integer>> deviceRuleMatch;
    public Map<String, Map<ForwardAction, Integer>> devicePortPredicate;

    public Map<String, HashSet<Lec>> deviceLecs;

    static public Map<String, Integer> devicePacketSpace;
    public Map<String, Map<Rule, List<Integer>>> deviceRuleBlacklist;


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


    public void putDeviceIfAbsent(String name){
        if(!deviceRuleHit.containsKey(name)){
            this.deviceRuleHit.put(name, new HashMap<>());
            this.deviceRuleMatch.put(name, new HashMap<>());
            this.devicePortPredicate.put(name, new HashMap<>());
            this.deviceLecs.put(name, new HashSet<>());
            this.deviceRuleBlacklist.put(name, new HashMap<>());
        }
    }

    // put Attribute
    // get Attribute
    public void putDeviceRuleHit(String deviceName, Rule rule, int hit){
            deviceRuleHit.get(deviceName).put(rule, hit);
    }

    public void putDeviceRuleMatch(String deviceName, Rule rule, int match){
            deviceRuleMatch.get(deviceName).put(rule, match);
    }

    // get Attribute
    public int getDeviceRuleHit(String deviceName, Rule rule){
        return this.deviceRuleHit.get(deviceName).get(rule);
    }

    public int getDeviceRuleMatch(String deviceName, Rule rule){
        return this.deviceRuleMatch.get(deviceName).get(rule);
    }

    public HashSet<Lec> getDeviceLecs(String deviceName){
        return this.deviceLecs.get(deviceName);
    }

    public void setTrieAndBlacklist(String deviceName, List<Rule> rules){
        Trie trie = new Trie();
        trie.addAndGetAllOverlappingAndAddToBlacklist(rules, this, deviceName);
        trie = null;
    }

    // public void setTrieAndBlacklistIPV6(String deviceName, List<RuleIPV6> rulesIpv6s){
    //     Trie trie = new Trie();
    //     trie.addAndGetAllOverlappingAndAddToBlacklistIPV6(rulesIpv6s, this, deviceName);
    //     trie = null;
    // }

    public void putDeviceRuleBlacklist(String deviceName, Rule rule, Rule blackRule){
        if(!deviceRuleBlacklist.get(deviceName).containsKey(rule)){
            this.deviceRuleBlacklist.get(deviceName).put(rule, new ArrayList<>());
        }
        int black = this.deviceRuleMatch.get(deviceName).get(blackRule);
        this.deviceRuleBlacklist.get(deviceName).get(rule).add(black);
    }

    public List<Integer> getDeviceRuleBlacklist(String deviceName, Rule rule){
        if(!deviceRuleBlacklist.get(deviceName).containsKey(rule)){
            this.deviceRuleBlacklist.get(deviceName).put(rule, new ArrayList<>());
        }
        return this.deviceRuleBlacklist.get(deviceName).get(rule);
    }

    public void srcDvNetParseAllSpace(Map<String, List<IPPrefix>> spaces){
        BDDEngine bddEngine = this.getBddEngine();
        for(Map.Entry<String, List<IPPrefix>> entry : spaces.entrySet()) {
            String dstDevice = entry.getKey();
            List<IPPrefix> ipPrefixList = spaces.get(dstDevice);
            int s = bddEngine.encodeDstIPPrefixList(ipPrefixList);
            devicePacketSpace.put(dstDevice, s);
        }
    }

    public void srcDvNetParseAllSpaceIPV6(Map<String, List<IPPrefixIPV6>> spacesIPV6){
        BDDEngine bddEngine = this.getBddEngine();
        for(Map.Entry<String, List<IPPrefixIPV6>> entry : spacesIPV6.entrySet()) {
            String dstDevice = entry.getKey();
            List<IPPrefixIPV6> ipPrefixList = spacesIPV6.get(dstDevice);
            int s = bddEngine.encodeDstIPPrefixListIPV6(ipPrefixList);
            devicePacketSpace.put(dstDevice, s);
        }
    }

    public void copyBdd(BDDEngine srcBdd, String copyType) throws Exception {
        BDDEngine bddCopy = null;
        if(Objects.equals(copyType, "Reflect")){
            ReflectDeepCopy copyHelper = new ReflectDeepCopy();
            bddCopy = (BDDEngine) copyHelper.deepCopy(srcBdd);
        }
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
        this.bddEngine = bddCopy;
        for(Node node : nodesTable.values()){
            assert this.bddEngine != null;
            node.setBdd(this.bddEngine);
        }
    }

    public BDDEngine getBddEngine(){
        return this.bddEngine;
    }

    public void setPacketSpace(int s) {this.packetSpace = s;}

    public void setDstNode(Node node){
        this.dstNode = node;
    }


    public Node getDstNode() {return this.dstNode;}

    public void init(){
        this.deviceRuleHit = new HashMap<>();
        this.deviceRuleMatch = new HashMap<>();
        this.devicePortPredicate = new HashMap<>();
        this.deviceLecs = new HashMap<>();
        this.deviceRuleBlacklist = new HashMap<>();
        devicePacketSpace = new HashMap<>();
    }



}
