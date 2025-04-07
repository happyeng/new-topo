package org.sngroup.verifier;


import org.sngroup.util.ForwardAction;
import org.sngroup.util.ForwardType;

import java.util.*;

public class Lec{
    public ForwardType type;
    public int predicate;
    public ForwardAction forwardAction;

    public Lec(ForwardAction forwardAction, int predicate) {
        type = forwardAction.forwardType;
        this.predicate = predicate;
        this.forwardAction = forwardAction;
    }

    @Override
    public String toString() {
        return String.format("{%s, %s}", forwardAction, predicate);
    }

    public int getMemoryUsage(){
        return 8;
    }
}
