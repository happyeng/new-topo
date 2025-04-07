package org.sngroup.verifier;

import java.util.Collection;
import java.util.Objects;
import java.util.Vector;

public class Count {
    public Vector<Integer> count;
    public Count(Vector<Integer> count){
        this.count = count;
    }
    public Count(Count count){
        this.count = new Vector<>(count.count);
    }
    public Count(){
        this.count = new Vector<>();
        this.count.add(0);
    }

    public void set(Collection<Integer> count) {
        this.count = new Vector<>(count);
    }

    public void set(int num){
        this.count.clear();
        this.count.add(num);
    }

    public boolean isZero(){
        return this.count.size() == 1 && this.count.get(0) == 0;
    }
}
