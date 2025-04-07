package org.sngroup.verifier;

import org.sngroup.util.DevicePort;
//import org.sngroup.util.Event;

// 改写Context

public class Context {
    private CibMessage cib;

    public int topoId;

    public String deviceName;

    public void setTopoId(int topoId){this.topoId = topoId;}

    public CibMessage getCib() {
        return cib;
    }

    public void setCib(CibMessage cib) {
        this.cib = cib;
    }

    public void setDeviceName(String deviceName){this.deviceName = deviceName;}

    public String getDeviceName(){return this.deviceName;}


    public Context copy(){
        Context c = new Context();
        c.cib = this.cib;
        c.topoId = this.topoId;
        return c;
    }
}
