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

//import org.sngroup.test.runner.ThreadRunner;z
import jdd.bdd.BDD;
import org.sngroup.test.runner.TopoRunner;
import org.sngroup.test.runner.Runner;
import org.sngroup.util.*;

import java.io.*;
//import java.lang.invoke.DelegatingMethodHandle$Holder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class Device {

    public final String name;
    protected Network network2;

    public BDDEngine bddEngine;

    protected ThreadPool threadPool;

    public List<Rule> rules;

    public List<RuleIPV6> rulesIPV6;

    public static Map<String, HashSet<Lec>> globalLecs;

    public static Map<String, List<IPPrefix>> spaces;

    public static Map<String, List<IPPrefixIPV6>> spacesIPV6;

    private final Runner runner;

    public Device(String name, Network network, Runner runner, ThreadPool tp) {
        this.name = name;
        this.network2 = network;
        this.runner = runner;
        init();
        this.threadPool = tp;
    }

    // public void readOnlyRulesFile(String filename) {
    //     try {
    //         File file = new File(filename);
    //         InputStreamReader isr = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
    //         BufferedReader br = new BufferedReader(isr);
    //         String line;

    //         while ((line = br.readLine()) != null) {
    //             String[] token = line.split("\\s+");
    //             if (token[0].equals("fw") || token[0].equals("ALL") || token[0].equals("ANY")) {
    //                 // System.out.println("ishere ??????????????");
    //                 Collection<String> forward = new HashSet<>(); // 去掉端口名中“.”后的字符
    //                 for (int i = 3; i < token.length; i++) {
    //                     forward.add(token[i]);
    //                 }
    //                 long ip = Long.parseLong(token[1]);
    //                 int prefix = Integer.parseInt(token[2]);
    //                 ForwardType ft = token[0].equals("ANY") ? ForwardType.ANY : ForwardType.ALL;
    //                 Rule newRule = new Rule(ip, prefix, forward, ft);
    //                 this.rules.add(newRule);
    //                 System.out.println("新添加规则的forward type" + newRule.forwardAction.toString());
    //             }

    //         }

    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    //     // 加入默认路由
    //     // rules.add(new Rule(0, 0, ForwardAction.getNullAction()));
    // }

    public void readOnlyRulesFile(String filename) {
        try {
            File file = new File(filename);
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            String line;

            while ((line = br.readLine()) != null) {
                String[] token = line.split("\\s+");
                if (token[0].equals("fw") || token[0].equals("ALL") || token[0].equals("ANY") || token[0].equals("any")) {
                    Collection<String> forward = new HashSet<>(); // 去掉端口名中“.”后的字符
                    for (int i = 3; i < token.length; i++) {
                        forward.add(token[i]);
                    }
                    long ip = Long.parseLong(token[1]);
                    int prefix = Integer.parseInt(token[2]);
                    ForwardType ft = token[0].equals("ANY") ? ForwardType.ANY : ForwardType.ALL;
                    Rule newRule = new Rule(ip, prefix, forward, ft);
                    this.rules.add(newRule);
                    // System.out.println("新添加规则的forward type" + newRule.forwardAction.toString());
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        // 加入默认路由
        // rules.add(new Rule(0, 0, ForwardAction.getNullAction()));
    }

    public void readOnlyRulesFileIPV4_S(String filename) {
        try {
            File file = new File(filename);
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            String line;

            while ((line = br.readLine()) != null) {
                String[] token = line.split("\\s+");
                if (token[0].equals("fw") || token[0].equals("ALL") || token[0].equals("ANY") || token[0].equals("any")) {
                    Collection<String> forward = new HashSet<>(); // 去掉端口名中“.”后的字符
                    for (int i = 3; i < token.length; i++) {
                        forward.add(token[i].split("\\.", 2)[0]);
                    }
                    String ip = token[1];
                    int prefix = Integer.parseInt(token[2]);
                    ForwardType ft = (token[0].equals("ANY") || token[0].equals("any"))? ForwardType.ANY : ForwardType.ALL;
                    RuleIPV6 newRule = new RuleIPV6(ip, prefix, forward, ft);
                    this.rulesIPV6.add(newRule);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        // 加入默认路由
        this.rulesIPV6.add(new RuleIPV6("0.0.0.0", 0, ForwardAction.getNullAction()));
    }

    public void readOnlyRulesFileIPV6(String filename) {
        try {
            File file = new File(filename);
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            String line;

            while ((line = br.readLine()) != null) {
                String[] token = line.split("\\s+");
                if (token[0].equals("fw") || token[0].equals("ALL") || token[0].equals("ANY") || token[0].equals("any")) {
                    Collection<String> forward = new HashSet<>(); // 去掉端口名中“.”后的字符
                    for (int i = 3; i < token.length; i++) {
                        forward.add(token[i].split("\\.", 2)[0]);
                    }
                    String ip = token[1];
                    int prefix = Integer.parseInt(token[2]);
                    ForwardType ft = (token[0].equals("ANY") || token[0].equals("any"))? ForwardType.ANY : ForwardType.ALL;
                    RuleIPV6 newRule = new RuleIPV6(ip, prefix, forward, ft);
                    this.rulesIPV6.add(newRule);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        // 加入默认路由
        this.rulesIPV6.add(new RuleIPV6("::", 0, ForwardAction.getNullAction()));
    }

    public static void readOnlySpaceFileIPV6(String filename) {
        Map<String, List<IPPrefixIPV6>> spacesIPV6 = new HashMap<>();
        try {
            InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get(filename)),
                    StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            String line;

            while ((line = br.readLine()) != null) {
                if (line.startsWith("["))
                    continue;
                String[] token = line.split("\\s+");
                String device = token[0];
                String ip = token[1];
                int prefix = Integer.parseInt(token[2]);
                IPPrefixIPV6 space = new IPPrefixIPV6(ip, prefix);
                spacesIPV6.putIfAbsent(device, new LinkedList<>());
                spacesIPV6.get(device).add(space);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Device.spacesIPV6 = spacesIPV6;
    }

    public void encodeDeviceRule(DVNet dvNet) {
        long timePoint = System.currentTimeMillis();
        dvNet.putDeviceIfAbsent(name);
        dvNet.setTrieAndBlacklist(name, rules);
        BDDEngine tmpBddEngine = dvNet.getBddEngine();
        for (Rule rule : rules) {
            int tmpHit = tmpBddEngine.encodeIpWithoutBlacklist(dvNet.getDeviceRuleMatch(name, rule),
                    dvNet.getDeviceRuleBlacklist(name, rule));
            dvNet.putDeviceRuleHit(name, rule, tmpHit);
        }
        long timePoint1 = System.currentTimeMillis();
        // System.out.println("每个Device前缀匹配花费的时间" + (timePoint1 - timePoint) + "ms");
        encodeRuleToLec(dvNet);
        long timePoint2 = System.currentTimeMillis();
        // System.out.println("每个Device转化lec所花费的时间" + (timePoint2 - timePoint1) + "ms");
    }


    public void encodeDeviceRuleIPV6(DVNet dvNet) {
        long timePoint = System.currentTimeMillis();
        dvNet.putDeviceIfAbsent(name);
        dvNet.setTrieAndBlacklist(name, rules);
        BDDEngine tmpBddEngine = dvNet.getBddEngine();
        for (Rule rule : rules) {
            int tmpHit = tmpBddEngine.encodeIpWithoutBlacklist(dvNet.getDeviceRuleMatch(name, rule),
                    dvNet.getDeviceRuleBlacklist(name, rule));
            dvNet.putDeviceRuleHit(name, rule, tmpHit);
        }
        long timePoint1 = System.currentTimeMillis();
        // System.out.println("每个Device前缀匹配花费的时间" + (timePoint1 - timePoint) + "ms");
        encodeRuleToLec(dvNet);
        long timePoint2 = System.currentTimeMillis();
        // System.out.println("每个Device转化lec所花费的时间" + (timePoint2 - timePoint1) + "ms");
    }

    public static void readOnlySpaceFile(String filename) {
        Map<String, List<IPPrefix>> spaces = new HashMap<>();
        try {
            InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get(filename)),
                    StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            String line;

            while ((line = br.readLine()) != null) {
                if (line.startsWith("["))
                    continue;
                String[] token = line.split("\\s+");
                String device = token[0];
                long ip = Long.parseLong(token[1]);
                int prefix = Integer.parseInt(token[2]);
                IPPrefix space = new IPPrefix(ip, prefix);
                spaces.putIfAbsent(device, new LinkedList<>());
                spaces.get(device).add(space);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Device.spaces = spaces;
    }

    public void encodeRuleToLec(DVNet dvNet) {
        TSBDD bdd = dvNet.getBddEngine().getBDD();
        Map<ForwardAction, Integer> portPredicate = dvNet.devicePortPredicate.get(name);
        // 按动作进行等价类的合并
        for (Rule rule : rules) {
            Collection<String> tmpPorts = rule.forwardAction.ports;
            ForwardType tmpForwardType = rule.forwardAction.forwardType;
            for(String tmpPort : tmpPorts){
                ForwardAction tmpForwardAction = new ForwardAction(tmpForwardType, tmpPort) ;
            if (portPredicate.containsKey(tmpForwardAction)) {
                int newPredicate = bdd.orTo(portPredicate.get(tmpForwardAction), dvNet.getDeviceRuleHit(name, rule));
                portPredicate.put(tmpForwardAction, newPredicate);
            } else {
                portPredicate.put(tmpForwardAction, bdd.ref(dvNet.getDeviceRuleHit(name, rule)));
                // portPredicate.put(rule.forwardAction, dvNet.getDeviceRuleHit(name, rule));
            }
        }
        }
        HashSet<Lec> tmpLecs = new HashSet<>();
        for (Map.Entry<ForwardAction, Integer> kv : portPredicate.entrySet()) {
            tmpLecs.add(new Lec(kv.getKey(), kv.getValue()));
        }
        Device.globalLecs.put(name, tmpLecs);
    }

    public void encodeRuleToLecFromScratchToFinish(DVNet dvNet) {
        dvNet.putDeviceIfAbsent(name);
        Collections.sort(rules, prefixLenComparator); // 优先级排序
        Map<ForwardAction, Integer> portPredicate = dvNet.devicePortPredicate.get(name);
        BDDEngine bdd = dvNet.getBddEngine();
        TSBDD tsbdd = bdd.getBDD();
        boolean isFirst = false;
        int allBdd = 0;
        int lastPrefixLen = 0;
        int ruleCnt = 0;
        for (Rule rule : rules) {
            ruleCnt++;
            // 1. BDD转化
            int tmpMatch = bdd.encodeDstIPPrefix(rule.ip, rule.prefixLen);
            int tmpHit = tmpMatch;
            // 2. 最长前缀匹配 (算法改进, 先对前缀长度进行判断, 如果相同则一定无交集, 直接定下hit)
            if (rule.prefixLen == lastPrefixLen) {
                tmpHit = tmpMatch;
            } else {
                int tmp = tsbdd.not(allBdd);
                tmpHit = tsbdd.and(tmpMatch, tmp);
            }
            allBdd = tsbdd.orTo(allBdd, tmpHit);
            // dvNet.putDeviceRuleHit(name, rule, tmpHit);
            lastPrefixLen = rule.prefixLen;
            // 3. 合并为LEC
            if (portPredicate.containsKey(rule.forwardAction)) {
                // int newPredicate = tsbdd.orTo(portPredicate.get(rule.forwardAction),
                // dvNet.getDeviceRuleHit(name, rule));
                int newPredicate = tsbdd.orTo(portPredicate.get(rule.forwardAction), tmpHit);
                // int newPredicate = tsbdd.orTo(tmpHit, tmpHit);
                portPredicate.put(rule.forwardAction, newPredicate);
            } else {
                // portPredicate.put(rule.forwardAction, tsbdd.ref(dvNet.getDeviceRuleHit(name,
                // rule)));
                portPredicate.put(rule.forwardAction, tsbdd.ref(tmpHit));
            }
        }
        HashSet<Lec> tmpLecs = new HashSet<>();
        for (Map.Entry<ForwardAction, Integer> kv : portPredicate.entrySet()) {
            tmpLecs.add(new Lec(kv.getKey(), kv.getValue()));
        }
        Device.globalLecs.put(name, tmpLecs);
    }

    public void encodeRuleToLecFromScratch(DVNet dvNet) {
        dvNet.putDeviceIfAbsent(name);
        Collections.sort(rules, prefixLenComparator); // 优先级排序
        Map<ForwardAction, Integer> portPredicate = dvNet.devicePortPredicate.get(name);
        BDDEngine bdd = dvNet.getBddEngine();
        TSBDD tsbdd = bdd.getBDD();
        HashSet<String> portsSet = new HashSet<>();
        HashMap<String, Integer> portCnt = new HashMap<>();
        // for (Rule rule : rules) {
        //     // for (String str : rule.forwardAction.ports) {
        //     //     if (!portCnt.containsKey(str))
        //     //         portCnt.put(str, 1);
        //     //     else
        //     //         portCnt.put(str, portCnt.get(str) + 1);
        //     //     if (name.startsWith("HBHL_D1_310_01_03_09_DCI_0_0"))
        //     //         portsSet.add(str);
        //     // }
        //     // 1. BDD转化
        //     int tmpMatch = bdd.encodeDstIPPrefix(rule.ip, rule.prefixLen);
        //     dvNet.putDeviceRuleMatch(name, rule, tmpMatch);
        // }
        // 2、最长前缀匹配
        int allBdd = 0;
        boolean isFirst = true;
        for (Rule rule : rules) {
            int tmpMatch = bdd.encodeDstIPPrefix(rule.ip, rule.prefixLen);
            // dvNet.putDeviceRuleMatch(name, rule, tmpMatch);
            // int tmpMatch = dvNet.getDeviceRuleMatch(name, rule);
            int tmpHit = tmpMatch;
            // 2. 最长前缀匹配
            if (isFirst) {
                isFirst = false;
                allBdd = tsbdd.ref(tmpMatch);
                tsbdd.ref(tmpMatch);
            } else {
                int tmp = tsbdd.ref(tsbdd.not(allBdd));
                tmpHit = tsbdd.ref(tsbdd.and(tmpMatch, tmp));
                allBdd = tsbdd.orTo(allBdd, tmpMatch);

                tsbdd.deref(tmp);
            }
            dvNet.putDeviceRuleHit(name, rule, tmpHit);
        }
        // 3. 合并为LEC
        this.encodeRuleToLec(dvNet);
        // lec log to debug
        // if (name.startsWith("HBHL_D1_310_01_03_09_DCI_0_0")) {
        // List<String> portList = new ArrayList<>(portsSet);
        // Collections.sort(portList);
        // for(String str : portList){
        // System.out.println(str + " " + portCnt.get(str));
        // }
        // // System.out.print("tmpLecs的大小" + tmpLecs.size());
        // // System.out.println("规则数量" + this.rules.size());
        // // for(Lec lec : tmpLecs) System.out.println("端口名" +
        // lec.forwardAction.toString());
        // }
    }

    public void encodeRuleToLecFromScratchIPV6(DVNet dvNet) throws UnknownHostException {
        dvNet.putDeviceIfAbsent(name);
        Collections.sort(rulesIPV6, prefixLenComparatorIPV6); // 优先级排序
        Map<ForwardAction, Integer> portPredicate = dvNet.devicePortPredicate.get(name);
        BDDEngine bdd = dvNet.getBddEngine();
        TSBDD tsbdd = bdd.getBDD();
        boolean isFirst = false;
        int allBdd = 0;
        for (RuleIPV6 ruleIPV6 : rulesIPV6) {
            // 1. BDD转化
            int tmpMatch = bdd.encodeDstIPPrefixIpv6(ruleIPV6.ip, ruleIPV6.prefixLen);
            int tmpHit = tmpMatch;
            
            // 2. 最长前缀匹配
            if (!isFirst) {
                isFirst = true;
                allBdd = tsbdd.ref(tmpMatch);
            } else {
                tmpHit = tsbdd.diff(tmpMatch, allBdd);
                allBdd = tsbdd.orTo(allBdd, tmpMatch);
            }

            // if (isFirst) {
            //     isFirst = false;
            //     allBdd = tsbdd.ref(tmpMatch);
            //     tsbdd.ref(tmpMatch);
            // } else {
            //     int tmp = tsbdd.ref(tsbdd.not(allBdd));
            //     tmpHit = tsbdd.ref(tsbdd.and(tmpMatch, tmp));
            //     allBdd = tsbdd.orTo(allBdd, tmpMatch);

            //     tsbdd.deref(tmp);
            // }
            
            // 要把每一个 port 拆开
            Collection<String> tmpPorts = ruleIPV6.forwardAction.ports;
            ForwardType tmpForwardType = ruleIPV6.forwardAction.forwardType;
            for(String tmpPort : tmpPorts){
                ForwardAction tmpForwardAction = new ForwardAction(tmpForwardType, tmpPort) ;
                if (portPredicate.containsKey(tmpForwardAction)) {
                    int newPredicate = tsbdd.orTo(portPredicate.get(tmpForwardAction), tmpHit);
                    portPredicate.put(tmpForwardAction, newPredicate);
                } else {
                    portPredicate.put(tmpForwardAction, tsbdd.ref(tmpHit));
                    // portPredicate.put(rule.forwardAction, dvNet.getDeviceRuleHit(name, rule));
                }
            }
        
            // 3. 合并为LEC
            // if (portPredicate.containsKey(ruleIPV6.forwardAction)) {
            //     int newPredicate = tsbdd.orTo(portPredicate.get(ruleIPV6.forwardAction), tmpHit);
            //     portPredicate.put(ruleIPV6.forwardAction, newPredicate);
            // } else {
            //     portPredicate.put(ruleIPV6.forwardAction, tsbdd.ref(tmpHit));
            // }
        }
        
        HashSet<Lec> tmpLecs = new HashSet<>();
        for (Map.Entry<ForwardAction, Integer> kv : portPredicate.entrySet()) {
            tmpLecs.add(new Lec(kv.getKey(), kv.getValue()));
        }
        Device.globalLecs.put(name, tmpLecs);
    }

    Comparator<Rule> prefixLenComparator = new Comparator<Rule>() {
        @Override
        public int compare(Rule r1, Rule r2) {
            return r2.prefixLen - r1.prefixLen;
        }
    };

    Comparator<RuleIPV6> prefixLenComparatorIPV6 = new Comparator<RuleIPV6>() {
        @Override
        public int compare(RuleIPV6 r1, RuleIPV6 r2) {
            return r2.prefixLen - r1.prefixLen;
        }
    };

    public void close() {
    }

    /**
     * 初始化
     */
    protected void init() {
        ForwardType.init();
        rulesIPV6 = new ArrayList<>();
        bddEngine = new BDDEngine();
        rules = new ArrayList<>();
        globalLecs = new HashMap<>();
    }
}
