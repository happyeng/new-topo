package org.sngroup.util;

import java.util.*;

public class RuleIPV6{
    public ForwardAction forwardAction;
    public int prefixLen;
    public String ip;
    public int hit;
    public int match;
    public int lecIndex;

    public Map<Integer, Integer> hitTable;
    public Map<Integer, Integer> matchTable;

    public Map<Integer, Integer> lecIndexTable;

    public RuleIPV6(String ip, int prefixLen, Collection<String> forward, ForwardType forwardType){
        init();
        this.ip = ip;
        this.prefixLen = prefixLen;

        this.forwardAction = new ForwardAction(forwardType, new HashSet<>(forward));
        this.lecIndex = -1;
    }

    public RuleIPV6(String ip, int prefixLen, ForwardAction forwardAction){
        init();
        this.ip = ip;
        this.prefixLen = prefixLen;

        this.forwardAction = forwardAction;
        this.lecIndex = -1;
    }

    public RuleIPV6(String ip, int prefixLen, String forward){
        init();
        this.ip = ip;
        this.prefixLen = prefixLen;
        Set<String> f = new HashSet<>();
        f.add(forward);
        this.forwardAction = new ForwardAction(ForwardType.ALL, f);
        this.lecIndex = -1;
    }

    public void init(){
        hitTable = new HashMap<>();
        matchTable = new HashMap<>();
        lecIndexTable = new HashMap<>();
    }

    public int getPriority() {
        return prefixLen;
    }

    public int getHit(){
        return hit;
    }

    public void setHit(int hit) {
        this.hit = hit;
    }

    public void setMatch(int match) {
        this.match = match;
    }

    public  int getHitByNet(int netIndex){
        return hitTable.get(netIndex);
    }

    public  int getMatchByNet(int netIndex){
        return matchTable.get(netIndex);
    }

    @Override
    public String toString() {
        return String.format("%s %s %s %s %s", ip, prefixLen, forwardAction, match, hit);
    }

    public String getIPPreString(){
        return String.format("%s/%s", this.ip, this.prefixLen);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuleIPV6 ruleIPV6 = (RuleIPV6) o;
        return prefixLen == ruleIPV6.prefixLen && ip == ruleIPV6.ip && Objects.equals(forwardAction, ruleIPV6.forwardAction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(forwardAction, prefixLen, ip);
    }

    public int getMemoryUsage(){
        int l = 0;
//        forward.forEach(f->l+=f.length());
        return 24 + l;
    }
}
