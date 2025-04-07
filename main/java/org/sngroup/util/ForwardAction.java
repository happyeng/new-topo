package org.sngroup.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;

public class ForwardAction {
    public ForwardType forwardType;
    public Collection<String> ports;
    private static final ForwardAction nullAction = new ForwardAction(ForwardType.DROP, new HashSet<>());

    public ForwardAction(ForwardType forwardType, Collection<String> ports){
        this.forwardType = forwardType;
        this.ports = ports;
    }

    public ForwardAction(ForwardType forwardType, String port) {
        this.forwardType = forwardType;
        this.ports = Collections.singletonList(port);
    }

    // Copy Function
    public ForwardAction(ForwardAction oriForward){
        this.forwardType = oriForward.forwardType;
        this.ports = new ArrayList<>();
        this.ports.addAll(oriForward.ports);
    }

    public static ForwardAction getNullAction(){
        return nullAction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForwardAction that = (ForwardAction) o;
        return forwardType == that.forwardType && ports.equals(that.ports);
    }

    @Override
    public int hashCode() {
        return Objects.hash(forwardType, ports);
    }

    @Override
    public String toString() {
        return "{" + forwardType +  ports + '}';
    }
}
