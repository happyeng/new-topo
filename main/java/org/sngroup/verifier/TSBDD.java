package org.sngroup.verifier;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jdd.bdd.BDD;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TSBDD implements Cloneable, Serializable {
    public BDD bdd;

    public TSBDD(){

    }

    public TSBDD(BDD bdd){
        this.bdd = bdd;
    }

    @Override
    public Object clone() {
        TSBDD tsbdd = null;
        try{
            tsbdd = (TSBDD) super.clone();
        }catch(CloneNotSupportedException e) {
            e.printStackTrace();
        }
        tsbdd.bdd = (BDD)this.bdd.clone();
        return tsbdd;
    }



    public  int cnt = 0;

    public int and(int u1, int u2){
        cnt++;
//        synchronized (bdd){
            return bdd.and(u1, u2);
//        }
    }

    public int andTo(int u1, int u2){
        cnt++;
//        synchronized (bdd){
            return bdd.andTo(u1, u2);
//        }
    }

    public int xor(int u1, int u2){
        cnt++;
//        synchronized (bdd){
            return bdd.xor(u1, u2);
//        }
    }

    public int or(int u1, int u2){
        cnt++;
//        synchronized (bdd){
            return bdd.or(u1, u2);
//        }
    }

    public int orTo(int u1, int u2){
        cnt++;
//        synchronized (bdd){
            return bdd.orTo(u1, u2);
//        }
    }

    public int not(int u1){
        cnt++;
//        synchronized (bdd){
            return bdd.not(u1);
//        }
    }
    public int diff(int u1, int u2){
        int ret;
//        synchronized (bdd) {
            int tmp = bdd.ref(bdd.not(u2));
            ret = bdd.ref(bdd.and(u1, tmp));
            bdd.deref(tmp);
//        }
        return ret;
    }

    public int ref(int u1){
//        synchronized (bdd){
            return bdd.ref(u1);
//        }
    }

    public int deref(int u1){
//        synchronized (bdd) {
            return bdd.deref(u1);
//            return 0;
//        }
    }

    public int createVar(){
//        synchronized (bdd){
            return bdd.createVar();
//        }
    }

    public boolean isValid(int u){
//        synchronized (bdd) {
            return bdd.isValid(u);
//        }
    }

    public int nodeCount(int u){
//        synchronized (bdd){
            return bdd.nodeCount(u);
//        }
    }

    public int getVarUnmasked(int u){
//        synchronized (bdd) {
            return bdd.getVarUnmasked(u);
//        }
    }

    public int getLow(int u){
//        synchronized (bdd) {
            return bdd.getLow(u);
//        }
    }

    public int getHigh(int u){
//        synchronized (bdd) {
            return bdd.getHigh(u);
//        }
    }

    public int mk(int u, int v, int w){
//        synchronized (bdd){
            return bdd.mk(u, v, w);
//        }
    }

    public void gc(){
//        synchronized (bdd){
            bdd.gc();
//        }
    }

    public int exists(int u, int cube){
//        synchronized (bdd){
            return bdd.exists(u, cube);
//        }
    }

    public long getMemoryUsage(){
//        synchronized (bdd) {
            return bdd.getMemoryUsage();
//        }
    }

    public void print(int u){
//        synchronized (bdd) {
            bdd.printSet(u);
//        }
    }
}
