package org.sngroup.util;

import java.util.*;

public class Rule{
    public ForwardAction forwardAction;
    public int prefixLen;
    public long ip;
    public int hit;
    public int match;
    public int lecIndex;

    public Map<Integer, Integer> hitTable;
    public Map<Integer, Integer> matchTable;

    public Map<Integer, Integer> lecIndexTable;

    public Rule(long ip, int prefixLen, Collection<String> forward, ForwardType forwardType){
        init();
        this.ip = ip;
        this.prefixLen = prefixLen;

        this.forwardAction = new ForwardAction(forwardType, new HashSet<>(forward));
        this.lecIndex = -1;
    }

    public Rule(long ip, int prefixLen, ForwardAction forwardAction){
        init();
        this.ip = ip;
        this.prefixLen = prefixLen;

        this.forwardAction = forwardAction;
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
}
