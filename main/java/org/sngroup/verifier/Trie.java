package org.sngroup.verifier;

import org.sngroup.util.Rule;

import java.util.ArrayList;
import java.util.List;

class TrieNode {
    ArrayList<Rule> rules;

    TrieNode left, right;

    public TrieNode() {
        rules = new ArrayList<>();
        left = right = null;
    }

    public TrieNode getNext(int flag) {
        if (flag == 0) {
            if (this.left == null) {
                this.left = new TrieNode();
            }
            return this.left;
        } else {
            if (this.right == null) {
                this.right = new TrieNode();
            }
            return this.right;
        }
    }

    public void add(Rule rule) {
        this.rules.add(rule);
    }

    public ArrayList<Rule> getRules() {
        return this.rules;
    }

    public void explore(ArrayList<Rule> ret) {
        if (this.left != null) this.left.explore(ret);
        if (this.right != null) this.right.explore(ret);
        ret.addAll(this.getRules());
    }
}

public class Trie {
    TrieNode root;

    public Trie() {
        this.root = new TrieNode();
    }

    public ArrayList<Rule> addAndGetAllOverlappingWith(Rule rule) {
        TrieNode t = this.root;
        ArrayList<Rule> ret = new ArrayList<>(t.getRules());
        long dstIp = rule.ip;
        long bit = 1L << 31;
        for (int i = 0; i < rule.getPriority(); i++) {
            boolean flag = (bit & dstIp) == 0;
            t = t.getNext(flag ? 0 : 1);
            bit >>=1;
            ret.addAll(t.getRules());
        }
        t.explore(ret);
        t.add(rule);
        return ret;
    }

    public ArrayList<Rule> dvNetAddAndGetAllOverlappingWith(Rule rule) {
        TrieNode t = this.root;
        ArrayList<Rule> ret = new ArrayList<>(t.getRules());

        long dstIp = rule.ip;
        long bit = 1L << 31;
        for (int i = 0; i < rule.getPriority(); i++) {

            boolean flag = (bit & dstIp) == 0;
            t = t.getNext(flag ? 0 : 1);
            bit >>=1;
            ret.addAll(t.getRules());
        }

        t.explore(ret);
        t.add(rule);
        return ret;
    }

    public void addAndGetAllOverlappingAndAddToBlacklist(List<Rule> allRules, DVNet dvNet, String devicename){
        BDDEngine bdd = dvNet.getBddEngine();
        for(Rule rule:allRules){
            int tmatch = bdd.encodeDstIPPrefix(rule.ip, rule.prefixLen);
            dvNet.putDeviceRuleMatch(devicename, rule, tmatch);
            List<Rule> overLapRs = addAndGetAllOverlappingWith(rule);
            for (Rule overr : overLapRs) {
                // 最长前缀匹配
                if (overr.getPriority() > rule.getPriority()) { // rule 包含的范围更大，最后 hit 只保留多余的部分
                    dvNet.putDeviceRuleBlacklist(devicename, rule, overr);
                }
                if (overr.getPriority() < rule.getPriority()) {
                    dvNet.putDeviceRuleBlacklist(devicename, overr, rule);
                }
            }
        }
    }

    public void addAndGetAllOverlappingAndAddToBlacklistIPV6(List<Rule> allRules, DVNet dvNet, String devicename){
        BDDEngine bdd = dvNet.getBddEngine();
        for(Rule rule:allRules){
            int tmatch = bdd.encodeDstIPPrefix(rule.ip, rule.prefixLen);
            dvNet.putDeviceRuleMatch(devicename, rule, tmatch);
            List<Rule> overLapRs = addAndGetAllOverlappingWith(rule);
            for (Rule overr : overLapRs) {
                // 最长前缀匹配
                if (overr.getPriority() > rule.getPriority()) { // rule 包含的范围更大，最后 hit 只保留多余的部分
                    dvNet.putDeviceRuleBlacklist(devicename, rule, overr);
                }
                if (overr.getPriority() < rule.getPriority()) {
                    dvNet.putDeviceRuleBlacklist(devicename, overr, rule);
                }
            }
        }
    }

    public void addAndGetAllOverlappingAndAddToBlacklist(List<Rule> allRules, TopoNet dvNet, String devicename){
        BDDEngine bdd = dvNet.getBddEngine();
        for(Rule rule:allRules){
            int tmatch = bdd.encodeDstIPPrefix(rule.ip, rule.prefixLen);
            dvNet.putDeviceRuleMatch(devicename, rule, tmatch);

            List<Rule> overLapRs = addAndGetAllOverlappingWith(rule);
            for (Rule overr : overLapRs) {
                // 最长前缀匹配
                if (overr.getPriority() > rule.getPriority()) { // rule 包含的范围更大，最后 hit 只保留多余的部分
                    dvNet.putDeviceRuleBlacklist(devicename, rule, overr);
                    // rule.addToBlacklist(overr.match);
                }
                if (overr.getPriority() < rule.getPriority()) {
                    dvNet.putDeviceRuleBlacklist(devicename, overr, rule);
                    // overr.addToBlacklist(rule.match);
                }
            }
        }
    }



}
