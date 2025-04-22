package org.sngroup.verifier;

import org.apache.commons.lang3.ObjectUtils;
import org.sngroup.test.runner.Runner;
import org.sngroup.test.runner.TopoRunner;
import org.sngroup.util.*;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

public class TopoNet extends DVNet {

    static public Set<Device> edgeDevices = new HashSet<>();

    static public Map<String, HashSet<DevicePort>> devicePortsTopo;

    public int topoCnt;

    public Set<Node> srcNodes;
    public Device dstDevice;

    public static Network network;

    public Invariant invariant;

    public TopoNet(Device dstDevice, int topoCnt) {
        super();
        init();
        this.dstDevice = dstDevice;
        this.topoCnt = topoCnt;
    }

    public void setInvariant(String packetSpace, String match, String path) {
        this.invariant = new Invariant(packetSpace, match, path);
    }

    public void nodeCalIndegree() {
        for (Node node : nodesTable.values()) {
            node.topoNetStart();
        }
    }

    public static void transformDevicePorts(Map<String, Map<String, Set<DevicePort>>> devicePortsOriginal) {
        // 双向连接
        Map<String, HashSet<DevicePort>> devicePortsNew = new HashMap<>();
        for (Map.Entry<String, Map<String, Set<DevicePort>>> entry : devicePortsOriginal.entrySet()) {
            String key = entry.getKey();
            Map<String, Set<DevicePort>> innerMap = entry.getValue();
            HashSet<DevicePort> connectedPorts = new HashSet<>();
            for (Set<DevicePort> portSet : innerMap.values()) {
                for(DevicePort port : portSet){
                    if(!connectedPorts.contains(port)) connectedPorts.add(port);
                }
            }
            devicePortsNew.put(key, connectedPorts);
        }
        devicePortsTopo = devicePortsNew;
    }

    public static void setNextTable() {
        for (Device device : TopoRunner.devices.values()) {
            String deviceName = device.name;
            if (!Node.nextTable.containsKey(deviceName))
                Node.nextTable.put(deviceName, new HashSet<>());
            HashSet<NodePointer> next = Node.nextTable.get(deviceName);
            for (Map.Entry<String, Set<DevicePort>> entry : network.devicePorts.get(device.name).entrySet()) {
                for (DevicePort dp : entry.getValue()) {
                    next.add(new NodePointer(dp.getPortName(), -1));
                }
            }
        }
    }

    public Node getDstNodeByName(String deviceName) {
        return this.nodesTable.get(deviceName);
    }

    public Boolean getAndSetBddEngine(LinkedBlockingDeque<BDDEngine> sharedQue) {
        boolean reused = false;
        synchronized (sharedQue) {
            if (sharedQue.size() != 0) {
                try {
                    this.bddEngine = sharedQue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                reused = true;
            }
        }
        return reused;
    }

    public void setNodeBdd() {
        for (Node node : nodesTable.values()) {
            node.setBdd(this.bddEngine);
        }
    }

    public void startCount(LinkedBlockingDeque<BDDEngine> sharedQue) {
        Context c = new Context();
        c.topoId = this.topoCnt;
        // dfs or bfs
        // this.getDstNode().startCountByDfs(c);
        this.getDstNode().bfsByIteration(c);
        for (Node node : srcNodes) {
            node.showResult();
        }
        synchronized (sharedQue) {
            try {
                sharedQue.put(this.getBddEngine());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void init() {
        srcNodes = new HashSet<>();
        this.nodesTable = new HashMap<>();
    }
}
