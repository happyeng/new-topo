/*
 * This program is free software: you can redistribute it and/or modify it under the terms of
 *  the GNU General Public License as published by the Free Software Foundation, either
 *   version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *   PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this
 *  program. If not, see <https://www.gnu.org/licenses/>.
 *
 * Authors: Chenyang Huang (Xiamen University) <xmuhcy@stu.xmu.edu.cn>
 *          Qiao Xiang     (Xiamen University) <xiangq27@gmail.com>
 *          Ridi Wen       (Xiamen University) <23020211153973@stu.xmu.edu.cn>
 *          Yuxin Wang     (Xiamen University) <yuxxinwang@gmail.com>
 */

package org.sngroup.verifier;

import org.sngroup.Configuration;
import org.sngroup.test.runner.Runner;
import org.sngroup.test.runner.TopoRunner;
import org.sngroup.util.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;

public class Node {

    public static AtomicInteger numDpvnet = new AtomicInteger(1);

    public static Map<DevicePort, DevicePort> topology = new HashMap<>();

    public static Map<String, Device> devices = new Hashtable<>();

    public Device device;
    public int index;
    public static Map<String, HashSet<NodePointer>> nextTable = new HashMap<>();

    String deviceName;

    public TopoNet topoNet;

    public TSBDD bdd;

    public boolean hasResult;

    public CibMessage lastResult;

    Invariant invariant;

    // int match_num;

    protected Set<CibTuple> todoList;

    protected Vector<CibTuple> locCib;
    protected Map<String, List<CibTuple>> portToCib;

    public boolean isDestination = false;

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public Node(Device device, int index, Invariant invariant, Runner runner) {
        this.device = device;
        this.index = index;
        this.invariant = invariant;
        // ------------------------------------------------------------ 2
        // -------------------------------------------------------------------------//
        hasResult = false;
        todoList = new HashSet<>();
        locCib = new Vector<>();
        portToCib = new Hashtable<>();
        lastResult = null;
    }

    public Node(Device device, TopoNet topoNet) {
        this.device = device;
        this.topoNet = topoNet;
        this.invariant = topoNet.invariant;
        this.index = topoNet.topoCnt;
        this.deviceName = device.name;
        // ------------------------------------------------------------ 2
        // -------------------------------------------------------------------------//
        hasResult = false;
        todoList = new HashSet<>();
        locCib = new Vector<>();
        portToCib = new Hashtable<>();
        lastResult = null;
    }

    int getPacketSpace() {
        return topoNet.packetSpace;
    }

    public void setBdd(BDDEngine bddEngine) {
        this.bdd = bddEngine.getBDD();
    }

    public void setTopoNet(TopoNet topoNet) {
        this.topoNet = topoNet;
    }

    public void topoNetStart() {
        initializeCibByTopo();
        // match_num = Integer.parseInt(invariant.getMatch().split("\\s+")[2]);
    }

    public boolean checkIsSrcNode() {
        TopoNet topoNet = this.topoNet;
        return topoNet.srcNodes.contains(this);
    }

    public boolean updateLocCibByTopo(String from, Collection<Announcement> announcements) {
        // System.out.println("端口 " + from + " 的 announcements 数量: " + announcements.size());
        
        boolean newResult = false;
        Queue<CibTuple> queue = new LinkedList<>(portToCib.get(from));
        // System.out.println("port to cib 计数 " + portToCib.size());
        // System.out.println("端口 " + from + " 的初始 queue 大小: " + portToCib.get(from).size());
        if(queue.size() == 0) return true;
        while (!queue.isEmpty()) {
            CibTuple cibTuple = queue.poll();
            // System.out.println("节点 " + from + " 的当前 queue 大小: " + queue.size());
            
            for (Announcement announcement : announcements) {
                int intersection = bdd.ref(bdd.and(announcement.predicate, cibTuple.predicate));
                if (intersection != cibTuple.predicate) {
                    // System.out.println("节点 " + from + " 的交集计算失败: 原始谓词 " + cibTuple.predicate + ", 新交集 " + intersection);
                    CibTuple newCibTuple = cibTuple.keepAndSplit(intersection, bdd); // 拆分CIBTuple
                    addCib(newCibTuple);
                    if (!hasResult && todoList.contains(cibTuple))
                        todoList.add(newCibTuple);
                    queue.add(newCibTuple);
                    // System.out.println("节点 " + from + " 的 queue 大小在添加 newCibTuple 后: " + queue.size());
                    return false;
                }
                newResult |= cibTuple.set(from, new Count(announcement.count));
                // newResult = true;
                if (cibTuple.isDefinite()) {
                    todoList.remove(cibTuple);
                    // System.out.println("节点 " + from + " 的 CIBTuple 已确定，移出 todoList。");
                    break;
                }
            }
        }
        if (!newResult) {
            // System.out.println("节点 " + from + " 的交集计算未产生新结果。");
        }
        return newResult;
    }
    
    

    protected void addCib(CibTuple cib) {
        locCib.add(cib);
        updateActionPortTable(cib);
    }

    private void updateActionPortTable(CibTuple cib) {
        for (String port : cib.action.ports) {
            portToCib.putIfAbsent(port, new Vector<>());
            portToCib.get(port).add(cib);
            List<CibTuple> tmpPortCibs = portToCib.get(port);
            // System.out.println("节点 " + this.deviceName + "更新端口后: " + port + "port to cib 大小" + tmpPortCibs.size());
        }
    }
    

    // 根据LEC和该节点的下一跳初始化LocCIB表
    public void initializeCibByTopo() {
        for (NodePointer np : Node.nextTable.get(deviceName)) {
            portToCib.put(np.name, new Vector<>());
        }
        // 如果是最终节点， 则直接设置结果为1
        if (isDestination) {
            CibTuple _cibTuple = new CibTuple(getPacketSpace(), ForwardAction.getNullAction(), 0);
            _cibTuple.count.set(1);
            addCib(_cibTuple);
            return;
        }
        int cnt = 0;
        for (Lec lec : topoNet.getDeviceLecs(device.name)) {
            if (!isDestination && lec.forwardAction.ports.size() == 1) { // 只需记录具有端口的lec
                // 只计算与下一跳有关的LEC
                int intersection = bdd.and(lec.predicate, getPacketSpace());
                if (intersection != 0) {
                    cnt += 1;
                    CibTuple cibTuple = new CibTuple(intersection, lec.forwardAction, 1);
                    addCib(cibTuple);
                    todoList.add(cibTuple);
                }
            }
        }
        // System.out.println("dstnode的名字 " + this.topoNet.getDstNode().deviceName + "
        // 当前node的名字 " + this.deviceName + " 入度节点的个数： " + cnt + " lecs的总个数 " +
        // topoNet.getDeviceLecs(deviceName).size());
    }

    // 从LocCIB中导出CIBOut
    public Map<Count, Integer> getCibOut() {
        Map<Count, Integer> cibOut = new HashMap<>();
        for (CibTuple cibTuple : locCib) {
            if (cibTuple.predicate == 0)
                continue;
            if (cibOut.containsKey(cibTuple.count)) {
                int pre = cibOut.get(cibTuple.count);
                pre = bdd.orTo(pre, cibTuple.predicate);
                cibOut.put(cibTuple.count, pre);
            } else {
                cibOut.put(cibTuple.count, cibTuple.predicate);
            }
        }
        return cibOut;
    }

    // ---------------------------------------------------------
    // 接收+发送------------------------------------------------------

    public void bfsByIteration(Context c) {
        Announcement a = new Announcement(0, getPacketSpace(), Utility.getOneNumVector(1));
        Vector<Announcement> al = new Vector<>();
        al.add(a);
        CibMessage cibOut = new CibMessage(al, new ArrayList<>(), index);
        c.setCib(cibOut);
        c.setDeviceName(deviceName);
        // 记录访问路径
        Set<String> visited = new HashSet<>();
        Queue<Context> queue = new LinkedList<>(); // 使用队列替换栈
        queue.add(c);
        // int bfsCnt = 0;
        // int checkCount = 0;
        int bfsCnt = 0;
        int ctxCnt = 0;
        int checkCnt = 0;
        System.out.println("终结点开始验证: " + c.getDeviceName());
    
        while (!queue.isEmpty()) {
            bfsCnt++;
            int size = queue.size();
            // System.out.println("当前层数中的节点" + size);
            for (int i = 0; i < size; i++) {
                Context currentCtx = queue.poll(); // 出队列
                String curDeviceName = currentCtx.getDeviceName();
                visited.add(curDeviceName);
                // System.out.println("BFS层次: " + bfsCnt + ", 当前设备: " + curDeviceName);
                HashSet<DevicePort> ps = TopoNet.devicePortsTopo.get(curDeviceName);
    
                int satisfiedCount = 0;
    
                if (ps != null) {
                    // System.out.println("当前设备的端口数量: " + ps.size());
                    for (DevicePort p : ps) {
                        if (p.portName.equals("temp")) {
                            System.out.println("temptemptemptemp");
                            continue;
                        }
                        DevicePort dst = topology.get(p);
                        if (dst != null) {
                            String dstDeviceName = dst.deviceName;
                            // System.out.println("检查下一跳结点: " + dstDeviceName);
                            checkCnt++;
    
                            if (!visited.contains(dstDeviceName)) {
                                Node dstNode = this.topoNet.getDstNodeByName(dst.deviceName);
                                Context ctx = new Context();
                                ctx.setCib(currentCtx.getCib());
                                int topoId = currentCtx.topoId;
                                ctx.setTopoId(topoId);
                                NodePointer np = new NodePointer(dst.getPortName(), topoId);
    
                                if (dstNode.countCheckByTopo(np, currentCtx)) {
                                    ctxCnt++;
                                    satisfiedCount++;
                                    // System.out.println("节点满足条件: " + dstNode.deviceName);
                                    List<Announcement> announcements = new LinkedList<>();
                                    Map<Count, Integer> nextCibOut = dstNode.getCibOut();
                                    for (Map.Entry<Count, Integer> entry : nextCibOut.entrySet())
                                        announcements.add(new Announcement(0, entry.getValue(), entry.getKey().count));
                                    CibMessage cibMessage = new CibMessage(announcements, new LinkedList<>(), index);
                                    ctx.setCib(cibMessage);
                                    ctx.setDeviceName(dstNode.deviceName);
                                    queue.add(ctx); // 入队列
                                    visited.add(dst.deviceName); // 访问标记放在这里
                                } else {
                                    // System.out.println("节点不满足条件: " + dstNode.deviceName);
                                }
                            } else {
                                // System.out.println("节点已访问: " + dstDeviceName);
                            }
                        } else {
                            // System.out.println("没有找到目标端口: " + p.toString());
                        }
                    }
                } else {
                    // System.out.println("当前设备没有端口信息: " + curDeviceName);
                }
    
                // System.out.println("节点 " + curDeviceName + " 结束后满足条件的节点个数: " + satisfiedCount);
            }
        }
        System.out.println("BFS结束，总遍历次数: " + bfsCnt + ", 满足条件的节点数: " + ctxCnt + ", 总检查次数: " + checkCnt);
    }
    

    protected boolean countCheckByTopo(NodePointer from, Context ctx) {
        CibMessage message = ctx.getCib();
        if (message != null) {
            // 1. 交集检查
            if (locCib.size() == 0) {
                // System.out.println("节点 " + this.deviceName + " 的 locCib 为空，无法继续传播。");
                return false;
            }
            if (!updateLocCibByTopo(from.name, message.announcements)) {
                // System.out.println("节点 " + this.deviceName + " 无法更新 locCib，交集检查失败。");
                return false;
            }
            // 2. 检查是否到达接入层结点
            if (checkIsSrcNode()) {
                // System.out.println("节点 " + this.deviceName + " 到达接入层结点，停止传播。");
                return false;
            }
            // 3. 拓扑排序, 只在满足todolist时继续传播
            if (!hasResult && todoList.isEmpty()) {
                // System.out.println("节点 " + this.deviceName + " 拓扑排序结果为空，继续传播。");
                return true;
            }
        }
        // System.out.println("节点 " + this.deviceName + " 无法满足传播条件，停止传播。");
        return false;
    }
    

    public void sendFirstResultByTopo(Context ctx, Set<String> visited) {
        List<Announcement> announcements = new LinkedList<>();
        Map<Count, Integer> cibOut = getCibOut();
        for (Map.Entry<Count, Integer> entry : cibOut.entrySet())
            announcements.add(new Announcement(0, entry.getValue(), entry.getKey().count));
        CibMessage cibMessage = new CibMessage(announcements, new LinkedList<>(), index);
        ctx.setCib(cibMessage);
        sendCountByTopo(ctx, visited);
        hasResult = true;
    }

    // 新函数:收到一个数据包就执行一次，对于其中每个FIB都要做一次
    protected void countByTopo(NodePointer from, Context ctx, Set<String> visited) {
        CibMessage message = ctx.getCib();
        // 检查是否是新结果
        // boolean isNew = storageAndCheckNew(from, message);
        if (message != null) {
            // Collection<Announcement> as = getAnnouncement(from);
            // 1. 检查某一端口的lec是否满足初始化时的packet space要求, 如果不满足则可直接判断不完全可达(包含交集检查)
            if (!updateLocCibByTopo(from.name, message.announcements)) {
                return;
            }
            // 2.检查是否到达接入层结点, 关键剪枝
            if (checkIsSrcNode()) {
                return;
            }
            // 3.拓扑排序, 只在满足todolist时往下递归, 关键剪枝
            if (!hasResult && todoList.isEmpty()) {
                sendFirstResultByTopo(ctx, visited);
            }
        }
    }

    public void sendCountByTopo(Context ctx, Set<String> visited) {
        lastResult = ctx.getCib();
        // new Func
        visited.add(deviceName);
        Collection<DevicePort> ps = TopoNet.devicePortsTopo.get(deviceName);
        if (ps != null) {
            for (DevicePort p : ps) {
                if (p.portName.equals("temp")) {
                    return;
                }
                transferByTopo(ctx, p, visited);
            }
            // 回溯
            visited.remove(deviceName);
        } else
            System.out.println("No Forwarding Port!");
    }

    public void transferByTopo(Context oldCtx, DevicePort sendPort, Set<String> visited) {
        DevicePort dst = topology.get(sendPort);
        Node dstNode = this.topoNet.getDstNodeByName(dst.deviceName);
        if (!visited.contains(dst.deviceName)) {
            // TODO: 跳数过大时,直接丢掉该包
            Context ctx = new Context();
            ctx.setCib(oldCtx.getCib());
            int topoId = oldCtx.topoId;
            ctx.setTopoId(topoId);
            NodePointer np = new NodePointer(dst.getPortName(), topoId);
            dstNode.countByTopo(np, ctx, visited);
        }
    }

    public void close() {
        // th.interrupt();
    }

    @Override
    public String toString() {
        // return getNodeName();
        return "";
    }

    public void showResult() {
        if (Configuration.getConfiguration().isShowResult()) {
            Map<Count, Integer> cibOut = getCibOut();
            int match_num = Integer.parseInt(invariant.getMatch().split("\\s+")[2]);
            final boolean[] success = { false };
            // System.out.println("entry key :" + entry.getKey() + " bdd value" +
            // entry.getValue() + "packet space value " + topoNet.packetSpace);
            // for (Map.Entry<Count, Integer> entry : cibOut.entrySet()) {
            // entry.getKey().count.forEach(i -> {
            // System.out.println(i); // 打印每个整数值
            // success[0] &= i >= match_num;
            // });
            // }

            for (Map.Entry<Count, Integer> entry : cibOut.entrySet()) {
                entry.getKey().count.forEach(i -> {
                    // System.out.println(i); // 打印每个整数值, 只要有一个端口能到达完整的ps，即认为可达
                    if (i >= match_num) {
                        success[0] = true;
                        return; // 退出 forEach 循环
                    }
                });
            }

            if (success[0]) {
                System.out.println("invariants: (" + invariant.getMatch() + ", " + invariant.getPath()
                        + ", packet space:" + topoNet.packetSpace + ") , result: " + success[0]);
                System.out.println("Num of DPVnets been verified: " + numDpvnet.getAndIncrement());
                // System.out.println("到达的节点名字" + this.deviceName);
            }
            // try {
            // // 加锁
            //
            // fileLock.lock();
            // // 打开文件并追加写入
            // BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH, true));
            //
            // // 写入线程名和一些内容
            // writer.write("invariants: (" + match + ", "+ invariant.getPath() + ", packet
            // space:" + packetSpace + ") , result: "+success[0] + "\n");
            //
            // // 关闭写入流
            // writer.close();
            // } catch (IOException e) {
            // e.printStackTrace();
            // } finally {
            // // 释放锁
            // fileLock.unlock();
            // }
        }
    }

}
