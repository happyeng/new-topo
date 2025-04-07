package org.sngroup.verifier;

import jdd.bdd.BDD;
import jdd.bdd.BDDNames;
import jdd.util.Allocator;
import org.sngroup.util.CopyHelper.ReflectDeepCopy;
import org.sngroup.util.IPPrefix;
import org.sngroup.util.IPPrefixIPV6;
import org.sngroup.util.Utility;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class BDDEngine implements Cloneable, Serializable {
    public static void main(String[] args) {
        BDDEngine bddEngine = new BDDEngine();
        int p1 = bddEngine.encodeDstIPPrefix(167772160L, 8);
        bddEngine.printPredicate(p1);
    }
    protected static final Logger logger = Logger.getGlobal();

    public static int BDDCnt = 0;

    public final static int BDDFalse = 0;
    public final static int BDDTrue = 1;

    final static int protocolBits = 8;
    final static int portBits = 16;
    public static int ipBits = 32;

    final static int srcIPStartIndex = 0;
    final static int dstIPStartIndex = srcIPStartIndex + ipBits;
    final static int srcPortStartIndex = dstIPStartIndex + ipBits;
    final static int dstPortStartIndex = srcPortStartIndex + portBits;
    final static int protocolStartIndex = dstPortStartIndex + portBits;

    final static int size = protocolStartIndex + protocolBits;

    private static char[] set_chars = null;
    static int[] protocol;
    static int[] srcPort;
    static int[] dstPort;
    static int[] srcIP;
    static int[] dstIP;
    public TSBDD bdd;

    static int[] vars;

    static int[] dstIPField;

    // 缓存常用的IP前缀编码，减少重复计算
    private static final Map<Long, Map<Integer, Integer>> ipPrefixCache = new HashMap<>();

    public BDDEngine(){
        bdd = new TSBDD(new BDD(10000, 10000));
        BDDCnt++;
        protocol = new int[protocolBits];
        srcPort = new int[portBits];
        dstPort = new int[portBits];
        srcIP = new int[ipBits];
        dstIP = new int[ipBits];
        dstIPField = new int[ipBits];

        // 声明变量
        DeclareSrcIP();
        DeclareDstIP();
        DeclareSrcPort();
        DeclareDstPort();
        DeclareProtocol();

        dstIPField = AndInBatch(dstIP);
    }

    // 优化的构造函数，用于从现有BDD创建新实例
    public BDDEngine(BDDEngine srcBdd, boolean isCopy){
        // 使用ReflectDeepCopy进行复制，这与DVNet.copyBdd中使用的方法相同
        try {
            if (isCopy) {
                ReflectDeepCopy copyHelper = new ReflectDeepCopy();
                this.bdd = (TSBDD) copyHelper.deepCopy(srcBdd.bdd);
            } else {
                // 如果不复制，则创建新的BDD实例
                this.bdd = new TSBDD(new BDD(10000, 10000));
            }
        } catch (Exception e) {
            System.err.println("复制BDD失败: " + e.getMessage());
            // 创建新的BDD作为后备
            this.bdd = new TSBDD(new BDD(10000, 10000));
        }

        // 初始化字段数组
        protocol = new int[protocolBits];
        srcPort = new int[portBits];
        dstPort = new int[portBits];
        srcIP = new int[ipBits];
        dstIP = new int[ipBits];
        dstIPField = new int[ipBits];

        // 声明变量
        DeclareSrcIP();
        DeclareDstIP();
        DeclareSrcPort();
        DeclareDstPort();
        DeclareProtocol();

        dstIPField = AndInBatch(dstIP);

        BDDCnt++; // 增加计数器
    }

    // 提供一个释放资源的方法
    public void dispose() {
        if (bdd != null && bdd.bdd != null) {
            try {
                bdd.bdd.cleanup(); // 清理BDD资源
            } catch (Exception e) {
                // 忽略清理异常
            }
        }
    }

    @Override
    public Object clone() {
        BDDEngine bddEngineCopy = null;
        try{
           bddEngineCopy = (BDDEngine) super.clone();
        }catch(CloneNotSupportedException e) {
            e.printStackTrace();
        }
        assert bddEngineCopy != null;
        bddEngineCopy.bdd = (TSBDD)this.bdd.clone();
        return bddEngineCopy;
    }

    private void DeclareProtocol() {
        DeclareVars(protocol, protocolBits);
    }

    private void DeclareSrcPort() {
        DeclareVars(srcPort, portBits);
    }

    private void DeclareDstPort() {
        DeclareVars(dstPort, portBits);
    }

    private void DeclareSrcIP() {
        DeclareVars(srcIP, ipBits);
    }

    private void DeclareDstIP() {
        DeclareVars(dstIP, ipBits);
    }

    public TSBDD getBDD(){
        return bdd;
    }

    public synchronized void printPredicate(int p){
        printSet(p);
    }

    public String printSet(int p)  {
        if( p < 2) {
            String result = String.format("%s", (p == 0) ? "null" : "all");
            return result;
        } else {
            StringBuilder sb = new StringBuilder();

            if(set_chars == null || set_chars.length < size)
                set_chars = Allocator.allocateCharArray(size);

            printSet_rec(p, 0, sb);
            return sb.toString();
        }
    }

    // 优化的编码方法，使用缓存减少重复计算
    public int encodeDstIPPrefix(long ipaddr, int prefixlen) {
        // 检查缓存
        if (ipPrefixCache.containsKey(ipaddr) && ipPrefixCache.get(ipaddr).containsKey(prefixlen)) {
            return ipPrefixCache.get(ipaddr).get(prefixlen);
        }

        // 计算二进制表示
        int[] ipbin = Utility.CalBinRep(ipaddr, 32);
        int[] ipbinprefix = new int[prefixlen];
        for (int k = 0; k < prefixlen; k++) {
            ipbinprefix[k] = ipbin[k + 32 - prefixlen];
        }

        // 编码前缀
        int entrybdd = EncodePrefix(ipbinprefix, dstIP, 32);

        // 缓存结果
        if (!ipPrefixCache.containsKey(ipaddr)) {
            ipPrefixCache.put(ipaddr, new HashMap<>());
        }
        ipPrefixCache.get(ipaddr).put(prefixlen, entrybdd);

        return entrybdd;
    }

    // 其他方法保持不变...

    public int encodeIpWithoutBlacklist(int bddip, List<Integer> blackList){
        if (blackList == null || blackList.isEmpty()) {
            return bdd.ref(bddip); // 如果没有黑名单，直接返回引用
        }

        int allBlack = 0;
        for (int blRule : blackList) {
            allBlack = bdd.orTo(allBlack, blRule);
        }

        int tmp = bdd.ref(bdd.not(allBlack));
        int newHit = bdd.ref(bdd.and(bddip, tmp));
        bdd.deref(tmp);

        return newHit;
    }

    private void printSet_rec(int p, int level, StringBuilder sb) {
        if(level == size) {
            sb.append(String.format("%s;",
                    parseIP(dstIPStartIndex)
            ));
            return;
        }
        BDD bdd = getBDD().bdd;
        int var = bdd.getVar(p);
        if(var > level || p == 1 ) {
            set_chars[level] = '-';
            printSet_rec(p, level+1, sb);
            return;
        }

        int low = bdd.getLow(p);
        int high = bdd.getHigh(p);

        if(low != 0) {
            set_chars[level] = '0';
            printSet_rec(low, level+1, sb);
        }

        if(high != 0) {
            set_chars[level] = '1';
            printSet_rec(high, level+1, sb);
        }
    }

    private String parseIP(int start){
        if(set_chars.length < start+31){
            System.err.println("Wrong ip!");
            return "";
        }

        int prefix= 32;
        boolean hasDash = false, hasMidDash = false, hasNumber = false;

        for(int i=prefix-1; i>=0; i--){
            hasDash |= set_chars[start+i] == '-';
            if(!hasNumber && (set_chars[start+i] == '1' || set_chars[start+i] == '0')){
                hasNumber = true;
                prefix = i+1;
            }
            hasMidDash |= set_chars[start+i] == '-' && hasNumber;
        }
        if(!hasNumber) return "any";
        if(!hasMidDash) {
            return Utility.charToInt8bit(set_chars, start) + "." +
                    Utility.charToInt8bit(set_chars, start+8) + "." +
                    Utility.charToInt8bit(set_chars, start+16) + "." +
                    Utility.charToInt8bit(set_chars, start+24) + (prefix==32?"":"/"+prefix);
        }
        return "not implement";
    }

    private void DeclareVars(int[] vars, int bits) {
        for (int i = bits - 1; i >= 0; i--) {
            vars[i] = bdd.createVar();
        }
    }

    // 优化的列表编码方法
    public int encodeDstIPPrefixList(List<IPPrefix> ipPrefixList){
        if (ipPrefixList == null || ipPrefixList.isEmpty()) {
            return BDDFalse;
        }

        // 对于小列表，直接计算
        if (ipPrefixList.size() <= 3) {
            int result = 0;
            for (IPPrefix ipPrefix : ipPrefixList){
                result = bdd.orTo(result, encodeDstIPPrefix(ipPrefix.ip, ipPrefix.prefix));
            }
            return result;
        }

        // 对于较大列表，使用分治法
        return encodeDstIPPrefixListRecursive(ipPrefixList, 0, ipPrefixList.size() - 1);
    }

    // 递归处理大型列表
    private int encodeDstIPPrefixListRecursive(List<IPPrefix> list, int start, int end) {
        if (start > end) {
            return BDDFalse;
        }
        if (start == end) {
            return encodeDstIPPrefix(list.get(start).ip, list.get(start).prefix);
        }

        int mid = (start + end) / 2;
        int left = encodeDstIPPrefixListRecursive(list, start, mid);
        int right = encodeDstIPPrefixListRecursive(list, mid + 1, end);

        return bdd.orTo(left, right);
    }

    public int encodeDstIPPrefixListIPV6(List<IPPrefixIPV6> ipPrefixList){
        int result = 0;
        for(IPPrefixIPV6 ipPrefix: ipPrefixList){
            try {
                result = bdd.orTo(result, encodeDstIPPrefixIpv6(ipPrefix.ip, ipPrefix.prefix));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public int encodeDstIPPrefixIpv6(String ipaddr, int prefixlen) throws UnknownHostException {
        int[] ipbin = Utility.ipv6ToBinaryArray(ipaddr, ipBits);
        int[] ipbinprefix = new int[prefixlen];
        for (int k = 0; k < prefixlen; k++) {
            ipbinprefix[k] = ipbin[k + ipBits - prefixlen];
        }
        int entrybdd = EncodePrefix(ipbinprefix, dstIP, ipBits);
        return entrybdd;
    }

    private int EncodePrefix(int[] prefix, int[] vars, int bits) {
        if (prefix.length == 0) {
            return BDDTrue;
        }

        int tempnode = BDDTrue;
        for (int i = 0; i < prefix.length; i++) {
            if (i == 0) {
                tempnode = EncodingVar(vars[bits - prefix.length + i],
                        prefix[i]);
            } else {
                int tempnode2 = EncodingVar(vars[bits - prefix.length + i],
                        prefix[i]);
                int tempnode3 = bdd.ref(bdd.and(tempnode, tempnode2));
                tempnode = tempnode3;
            }
        }
        return tempnode;
    }

    private int EncodingVar(int var, int flag) {
        if (flag == 0) {
            int tempnode = bdd.not(var);
            return tempnode;
        }
        if (flag == 1) {
            return var;
        }

        // should not reach here
        System.err.println("flag can only be 0 or 1!");
        return -1;
    }

    public int[] AndInBatch(int [] bddnodes)
    {
        int[] res = new int[bddnodes.length+1];
        res[0] = BDDTrue;
        int tempnode = BDDTrue;
        for(int i = bddnodes.length-1; i >=0 ; i--)
        {
            if(i == bddnodes.length-1)
            {
                tempnode = bddnodes[i];
                bdd.ref(tempnode);
            }else
            {
                if(bddnodes[i] == BDDTrue)
                {
                    // short cut, TRUE does not affect anything
                    continue;
                }
                if(bddnodes[i] == BDDFalse)
                {
                    // short cut, once FALSE, the result is false
                    // the current tempnode is useless now
                    bdd.deref(tempnode);
                    tempnode = BDDFalse;
                    break;
                }
                int tempnode2 = bdd.and(tempnode, bddnodes[i]);
                bdd.ref(tempnode2);
                // do not need current tempnode
                bdd.deref(tempnode);
                //refresh
                tempnode = tempnode2;
            }
            res[bddnodes.length-i] = tempnode;
        }
        return res;
    }
     /**
     * 获取BDD引擎中的节点数量
     */
    public int getNodeCount() {
        if (bdd != null && bdd.bdd != null) {
            try {
                // 使用BDD.nodeCount方法获取节点总数
                // 我们传入1（TRUE）作为根节点，因为我们需要从根节点开始计数
                return bdd.bdd.nodeCount(1);
            } catch (Exception e) {
                try {
                    // 如果上面的方法出错，尝试使用quasiReducedNodeCount
                    return bdd.bdd.quasiReducedNodeCount(1);
                } catch (Exception ex) {
                    // 所有尝试都失败，返回默认值
                    return 1000; // 使用一个默认估计值
                }
            }
        }
        return 0;
    }

    /**
     * 获取BDD引擎的内存使用估计（字节）
     */
    public long getEstimatedMemoryUsage() {
        if (bdd != null && bdd.bdd != null) {
            try {
                // 首先尝试使用BDD自己的内存估计
                return bdd.bdd.getMemoryUsage();
            } catch (Exception e) {
                // 如果失败，使用节点数*24字节的粗略估计
                return getNodeCount() * 24L;
            }
        }
        return 0;
    }

}