package org.sngroup.verifier;

import org.apache.commons.lang3.ObjectUtils;
import org.sngroup.test.runner.Runner;
import org.sngroup.test.runner.TopoRunner;
import org.sngroup.util.*;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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

    // 修改此方法以增强BDD复用
    public Boolean getAndSetBddEngine(LinkedBlockingDeque<BDDEngine> sharedQue) {
        boolean reused = false;
        try {
            long startTime = System.currentTimeMillis();

            // 等待更长时间，以增加获取BDD的机会
            BDDEngine engine = sharedQue.poll(10, TimeUnit.SECONDS);

            long waitTime = System.currentTimeMillis() - startTime;

            if (engine != null) {
                this.bddEngine = engine;
                reused = true;
                System.out.println("BDD引擎获取成功，等待时间: " + waitTime + "ms");
                return true;
            }

            // 如果队列为空，日志记录但不创建新引擎
            System.out.println("警告: BDD引擎池为空，将创建新的BDD引擎，等待时间: " + waitTime + "ms");

            // 返回false，让调用方创建新引擎
            return false;
        } catch (InterruptedException e) {
            System.err.println("等待BDD引擎被中断: " + e.getMessage());
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void setNodeBdd() {
        for (Node node : nodesTable.values()) {
            node.setBdd(this.bddEngine);
        }
    }

    public void startCount(LinkedBlockingDeque<BDDEngine> sharedQue) {
        Context c = new Context();
        c.topoId = this.topoCnt;

        try {
            // 执行BFS验证
            this.getDstNode().bfsByIteration(c);

            // 显示验证结果
            for (Node node : srcNodes) {
                node.showResult();
            }
        } catch (Exception e) {
            System.err.println("验证过程出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 无论成功还是失败，始终归还BDD引擎
            if (sharedQue != null && this.bddEngine != null) {
                try {
                    sharedQue.put(this.bddEngine);
                    //System.out.println("归还BDD引擎到池中");
                } catch (Exception ex) {
                    //System.err.println("归还BDD引擎失败: " + ex.getMessage());
                }
            }
        }
    }

    public void init() {
        srcNodes = new HashSet<>();
        this.nodesTable = new HashMap<>();
    }

}