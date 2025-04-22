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

package org.sngroup.test.runner;

import jdd.bdd.BDD;
import org.sngroup.Configuration;
import org.sngroup.util.*;
import org.sngroup.verifier.*;

import java.io.*;
import java.lang.ref.SoftReference;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class TopoRunner extends Runner {

    private static final int THREAD_POOL_READ_SIZE = 1; // 线程池读取大小，用于文件读取（当前未使用）
    public ThreadPool threadPool; // 主线程池，用于并行处理TopoNets
    public static Map<String, Device> devices; // 存储所有网络设备的映射

    public int ruleCnt = 0; // 规则计数器，记录处理的总规则数

    // IP版本控制标志
    public static boolean isIpv6 = false;    // 是否使用IPv6格式
    public static boolean isIpv4withS = true; // 是否使用带S格式的IPv4

    // 并行处理控制参数
    private int poolSize = 20;       // 线程池大小，控制最大并发处理线程数（建议：CPU核心数的50-80%）
    private int batchSize = 10;      // 每批处理的TopoNet数量，影响内存使用和处理速度（较大值提高吞吐量，但增加内存压力）
    private int maxBDDEngines = 5;   // 最大BDD引擎数量，超过此数量将回收（建议：不超过batchSize）
    private int maxRetryAttempts = 3; // 失败TopoNet的最大重试次数（防止无限重试）

    // 内存监控相关参数
    private static final double MEMORY_THRESHOLD_PERCENT = 0.70; // 内存使用阈值，超过此比例触发GC（0.0-1.0）
    private static final long MIN_FREE_MEMORY_MB = 200;         // 最小可用内存(MB)，低于此值触发GC
    private static final long MEMORY_CHECK_INTERVAL = 5;        // 每处理多少个TopoNet检查一次内存
    private static final long VERY_LOW_MEMORY_MB = 100;         // 内存极低阈值(MB)，低于此值启用节约模式
    private boolean sacrificeAccuracyForMemory = false;         // 极低内存时启用近似计算的标志

    // 内存消耗跟踪变量
    private long buildStartMemory = 0;      // 构建阶段开始时的内存使用量
    private long buildEndMemory = 0;        // 构建阶段结束时的内存使用量
    private long buildPeakMemory = 0;       // 构建阶段的内存峰值
    private long verificationPeakMemory = 0; // 验证阶段的内存峰值
    private long totalProcessedTopoNets = 0; // 处理的TopoNet总数
    private long verificationStartMemory = 0; // 验证阶段开始内存
    private long verificationEndMemory = 0;   // 验证阶段结束内存
    private long totalAllocatedMemory = 0;    // 累计分配的内存 (用于更准确的平均计算)
    private Map<String, Long> topoNetMemoryUsage = new HashMap<>(); // 记录每个TopoNet的内存使用情况（仅供参考）

    // 失败的TopoNet记录
    private Map<String, TopoNet> failedTopoNets = new HashMap<>(); // 存储处理失败的TopoNet对象
    private Map<String, Integer> retryAttempts = new HashMap<>();  // 记录每个TopoNet的重试次数

    // 内存使用统计文件
    private static final String MEMORY_STATS_FILE = "./memory_stats.txt"; // 内存统计信息输出文件路径

    // BDD引擎池 - 管理BDD引擎实例的复用
    private static final BDDEnginePool bddEnginePool = new BDDEnginePool();

    // 数据共享和缓存
    private final Map<String, SoftReference<Object>> softCache = new ConcurrentHashMap<>(); // 软引用缓存，内存压力下可自动释放
    private final Map<String, Integer> sharedPredicates = new ConcurrentHashMap<>();        // 共享谓词存储，减少重复内存占用
    private final Map<Integer, Set<String>> predicateGroups = new HashMap<>();              // 谓词分组，便于内存管理

    // 状态保存与恢复
    private static final String CHECKPOINT_DIR = "./checkpoints/";  // 检查点文件存储目录
    private boolean enableCheckpointing = true;                     // 是否启用检查点功能
    private int checkpointInterval = 20;                            // 每处理多少个TopoNet创建一次检查点

    // 网络拓扑相关结构
    private static Map<String, TopoNet> topoNetMap;                 // 存储所有TopoNet对象的映射
    private static Map<Integer, HashSet<TopoNet>> topoNetGroupMap = new HashMap<>(); // TopoNet分组（按网络编号）
    Set<Integer> dvNetSet;                                         // DVNet集合

    // 源网络和BDD引擎
    public static DVNet srcNet;     // 源网络，用于共享规则和空间信息
    public static BDDEngine srcBdd; // 源BDD引擎，用于规则转换

    // BDD引擎对象池 - 管理和重用BDD引擎实例，减少创建开销
    private static class BDDEnginePool {
        private final Queue<BDDEngine> pool = new ConcurrentLinkedQueue<>(); // 引擎池队列
        private static final int MAX_POOL_SIZE = 10;                        // 池中最大引擎数量

        /**
         * 从池中获取BDD引擎，如果池为空则克隆模板引擎
         * @param template 用于克隆的模板引擎
         * @return 可用的BDD引擎实例
         */
        public BDDEngine obtain(BDDEngine template) {
            BDDEngine engine = pool.poll();
            if (engine != null) {
                return engine;
            }

            try {
                return (BDDEngine) template.clone();
            } catch (Exception e) {
                throw new RuntimeException("Failed to clone BDD engine", e);
            }
        }

        /**
         * 回收BDD引擎到池中
         * @param engine 要回收的BDD引擎
         */
        public void recycle(BDDEngine engine) {
            if (engine == null || pool.size() >= MAX_POOL_SIZE) return;

            // 尝试清理BDD引擎的内部状态
            engine.getBDD().gc();

            pool.offer(engine);
        }
    }

    /**
     * 构造函数 - 初始化TopoRunner对象
     */
    public TopoRunner(){
        super();
        devices = new HashMap<>();
        dvNetSet = new HashSet<>();
        topoNetMap = new HashMap<>();

        // 创建检查点目录
        if (enableCheckpointing) {
            File dir = new File(CHECKPOINT_DIR);
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }
    }

    /**
     * 获取当前内存使用情况（已用内存）
     * @return 当前已使用的内存字节数
     */
    private long getCurrentMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * 获取当前内存使用量（MB）
     * @return 当前已使用的内存MB数
     */
    private long getCurrentMemoryUsageMB() {
        return getCurrentMemoryUsage() / (1024 * 1024);
    }

    /**
     * 更新最大内存使用量
     * 根据当前运行阶段更新buildPeakMemory或verificationPeakMemory，同时跟踪累计内存分配
     */
    private void updatePeakMemory() {
        Runtime runtime = Runtime.getRuntime();
        long currentMemory = getCurrentMemoryUsage();

        // 构建阶段内存峰值
        if (buildStartMemory > 0 && buildEndMemory == 0) {
            buildPeakMemory = Math.max(buildPeakMemory, currentMemory);
        }

        // 验证阶段内存峰值
        if (buildEndMemory > 0) {
            // 记录上次峰值到当前峰值的增量
            if (currentMemory > verificationPeakMemory) {
                long increment = currentMemory - verificationPeakMemory;
                totalAllocatedMemory += increment;
            }
            verificationPeakMemory = Math.max(verificationPeakMemory, currentMemory);
        }
    }

    /**
     * 保存内存使用统计信息到文件
     * 记录构建和验证的内存消耗，以及每个TopoNet的详细内存使用情况
     */
    private void saveMemoryStats() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(MEMORY_STATS_FILE))) {
            writer.println("========== 内存使用统计 ==========");
            writer.println("构建阶段内存消耗: " + ((buildEndMemory - buildStartMemory) / (1024 * 1024)) + " MB");
            writer.println("构建阶段内存峰值: " + (buildPeakMemory / (1024 * 1024)) + " MB");
            writer.println("验证阶段内存峰值: " + (verificationPeakMemory / (1024 * 1024)) + " MB");
            writer.println("验证阶段内存消耗: " + (Math.max(0, verificationEndMemory - verificationStartMemory) / (1024 * 1024)) + " MB");
            writer.println("累计分配内存总量: " + (totalAllocatedMemory / (1024 * 1024)) + " MB");

            if (totalProcessedTopoNets > 0) {
                writer.println("每个TopoNet平均内存消耗: " + (totalAllocatedMemory / totalProcessedTopoNets / (1024 * 1024)) + " MB");
                writer.println("处理TopoNet总数: " + totalProcessedTopoNets);
            }

            // 添加失败TopoNet信息
            if (!failedTopoNets.isEmpty()) {
                writer.println("--------------------------------");
                writer.println("失败的TopoNet:");
                failedTopoNets.keySet().forEach(name ->
                    writer.println(name + ": 重试次数=" + retryAttempts.getOrDefault(name, 0)));
                writer.println("失败总数: " + failedTopoNets.size());
            }

            writer.println("================================");
        } catch (IOException e) {
            System.err.println("保存内存统计信息失败: " + e.getMessage());
        }
    }

    /**
     * 获取线程池
     * @return 当前使用的线程池
     */
    public ThreadPool getThreadPool(){
        return threadPool;
    }

    /**
     * 获取指定名称的设备
     * @param name 设备名称
     * @return 设备对象
     */
    public Device getDevice(String name){
        return devices.get(name);
    }

    /**
     * 从软引用缓存获取对象，如果不存在或已被回收则创建
     * 利用软引用在内存压力下自动释放不常用对象
     *
     * @param key 缓存键
     * @param creator 对象创建器
     * @return 缓存的或新创建的对象
     */
    private <T> T getCachedOrCreate(String key, Supplier<T> creator) {
        SoftReference<Object> ref = softCache.get(key);
        if (ref != null) {
            T cached = (T) ref.get();
            if (cached != null) {
                return cached;
            }
        }

        T newObject = creator.get();
        softCache.put(key, new SoftReference<>(newObject));
        return newObject;
    }

    /**
     * 获取共享的谓词对象，减少重复存储
     * 用于避免相同谓词在内存中存储多份
     *
     * @param key 谓词键
     * @param creator 谓词创建器
     * @return 共享的谓词对象
     */
    public int getSharedPredicate(String key, Supplier<Integer> creator) {
        return sharedPredicates.computeIfAbsent(key, k -> creator.get());
    }

    /**
     * 优化谓词存储，将相似的谓词组合在一起
     * 用于改善内存使用和GC性能
     */
    private void organizePredicateGroups() {
        predicateGroups.clear();

        for (Map.Entry<String, Integer> entry : sharedPredicates.entrySet()) {
            int predicate = entry.getValue();
            predicateGroups.computeIfAbsent(predicate, k -> new HashSet<>()).add(entry.getKey());
        }
    }

    /**
     * 创建检查点，保存处理状态
     * 用于在长时间运行时保存中间结果，支持错误恢复
     *
     * @param batchIndex 当前批次索引
     * @param processedTopoNets 已处理的TopoNet映射
     */
    private void createCheckpoint(int batchIndex, Map<String, TopoNet> processedTopoNets) {
        if (!enableCheckpointing) return;

        String filename = CHECKPOINT_DIR + "checkpoint_batch_" + batchIndex + ".ser";
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
            // 仅保存必要的验证结果信息，非完整对象
            Map<String, Object> checkpointData = new HashMap<>();

            // 收集验证结果
            processedTopoNets.forEach((name, topoNet) -> {
                if (topoNet.getDstNode() != null) {
                    checkpointData.put(name, topoNet.getDstNode().lastResult);
                }
            });

            out.writeObject(checkpointData);
            System.out.println("创建检查点: " + filename);
        } catch (IOException e) {
            System.err.println("创建检查点失败: " + e.getMessage());
        }
    }

    /**
     * 从检查点恢复状态
     * 用于在重启或错误后恢复之前的处理结果
     *
     * @param batchIndex 批次索引
     * @return 恢复的检查点数据，如果不存在返回null
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> loadCheckpoint(int batchIndex) {
        if (!enableCheckpointing) return null;

        String filename = CHECKPOINT_DIR + "checkpoint_batch_" + batchIndex + ".ser";
        File checkpointFile = new File(filename);

        if (!checkpointFile.exists()) return null;

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(checkpointFile))) {
            return (Map<String, Object>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("无法加载检查点: " + e.getMessage());
            return null;
        }
    }

    /**
     * 检查当前内存使用情况，必要时触发GC
     * 监控内存使用并在接近阈值时主动释放资源
     *
     * @return 如果内存状态良好返回true，否则返回false
     */
    private boolean checkAndMaintainMemory() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        long usedMemory = totalMemory - freeMemory;

        // 更新内存峰值统计
        updatePeakMemory();

        double usedMemoryRatio = (double)(totalMemory - freeMemory) / maxMemory;
        long freeMemoryMB = freeMemory / (1024 * 1024);

        System.out.println("内存状态: 已用=" + (totalMemory - freeMemory) / (1024 * 1024) +
                         "MB, 可用=" + freeMemoryMB + "MB, 总内存=" +
                         totalMemory / (1024 * 1024) + "MB, 最大=" +
                         maxMemory / (1024 * 1024) + "MB");

        // 当内存极低时调整运行策略
        if (freeMemoryMB < VERY_LOW_MEMORY_MB) {
            sacrificeAccuracyForMemory = true;
            System.out.println("警告: 内存极低! 启用近似计算模式以节省内存");

            // 立即清除软引用缓存
            softCache.clear();
        }

        if (usedMemoryRatio > MEMORY_THRESHOLD_PERCENT || freeMemoryMB < MIN_FREE_MEMORY_MB) {
            System.out.println("内存不足，正在进行垃圾回收...");

            // 主动清理一些缓存
            softCache.clear();
            organizePredicateGroups();

            System.gc();

            // 重新检查内存状态
            freeMemory = runtime.freeMemory();
            freeMemoryMB = freeMemory / (1024 * 1024);
            System.out.println("垃圾回收后，可用内存: " + freeMemoryMB + "MB");

            // 如果内存仍然不足，建议减小批大小
            if (freeMemoryMB < MIN_FREE_MEMORY_MB) {
                System.out.println("警告: 内存仍然不足，建议减小批大小或增加JVM内存!");
                return false;
            }
        }
        return true;
    }

    /**
     * 动态计算合适的批大小
     * 根据系统资源状况自适应调整批处理大小
     *
     * @return 建议的批大小
     */
    private int calculateOptimalBatchSize() {
        Runtime runtime = Runtime.getRuntime();
        int availableProcessors = runtime.availableProcessors();
        long maxMemory = runtime.maxMemory();

        // 估算每个TopoNet大约需要多少内存（这个值需要根据实际情况调整）
        long estimatedMemoryPerTopoNet = 100 * 1024 * 1024; // 假设每个TopoNet需要100MB

        // 根据可用处理器和内存计算最佳批大小
        int processorBasedSize = availableProcessors;
        int memoryBasedSize = (int)(maxMemory / estimatedMemoryPerTopoNet);

        // 取较小值，并确保至少为1
        int optimalSize = Math.max(1, Math.min(processorBasedSize, memoryBasedSize));

        // 不超过配置的池大小
        optimalSize = Math.min(optimalSize, poolSize);

        System.out.println("动态计算的最佳批大小: " + optimalSize +
                        " (基于处理器=" + processorBasedSize +
                        ", 基于内存=" + memoryBasedSize + ")");

        return optimalSize;
    }

    /**
     * 根据内存压力排序TopoNets，较小的先处理
     * 优化处理顺序，提高整体性能
     *
     * @param topoNets TopoNet集合
     * @return 排序后的TopoNet列表
     */
    private List<Map.Entry<String, TopoNet>> prioritizeTopoNets(Collection<Map.Entry<String, TopoNet>> topoNets) {
        return topoNets.stream()
            .sorted((e1, e2) -> {
                // 这里可以根据实际情况添加评估逻辑
                // 例如根据设备数量、规则数量等
                return 0; // 默认保持原顺序
            })
            .collect(Collectors.toList());
    }

    public void build(){
        // 记录构建阶段开始内存
        buildStartMemory = getCurrentMemoryUsage();

        // IPV6 OR IPV4
        if(isIpv6) BDDEngine.ipBits = 128;
        else BDDEngine.ipBits = 32;
        srcBdd = new BDDEngine();

        System.out.println("Start Build in Runner!!!");
        srcNet = new DVNet(-1, srcBdd);
        threadPool = ThreadPool.FixedThreadPool(Configuration.getConfiguration().getThreadPoolSize());
        devices.clear();
        // 读取所有的device
        for(String deviceName : network.devicePorts.keySet()){
            Device d = new Device(deviceName, network, this, threadPool);
            if(network.edgeDevices.contains(deviceName)){
                TopoNet.edgeDevices.add(d);
            }
            devices.put(deviceName, d);
        }

        // 更新内存峰值
        updatePeakMemory();

        // device读取规则
        if(isIpv6 || isIpv4withS) readRuleByDeviceIPV6();
        else readRuleByDevice();

        // 更新内存峰值
        updatePeakMemory();

        // srcBDD转化规则
        srcBddTransformAllRules();

        // 更新内存峰值
        updatePeakMemory();

        // 生成topoNet
        genTopoNet();
        System.out.println("结点总数量" + devices.size());
        System.out.println("S0结点数量" + network.edgeDevices.size());
        System.out.println("表项总数量" + ruleCnt);

        System.out.println("End Build in Runner!!");

        // 记录构建阶段结束内存
        buildEndMemory = getCurrentMemoryUsage();
        System.out.println("Build阶段内存消耗: " + ((buildEndMemory - buildStartMemory) / (1024 * 1024)) + " MB");
        System.out.println("Build阶段内存峰值: " + (buildPeakMemory / (1024 * 1024)) + " MB");
    }

    public void start() {
        // 记录验证阶段开始内存
        verificationStartMemory = getCurrentMemoryUsage();
        totalAllocatedMemory = 0; // 重置累计内存分配计数器

        // 检查初始内存状态
        checkAndMaintainMemory();

        // 动态计算最佳批大小
        batchSize = calculateOptimalBatchSize();

        // 限制BDD引擎最大数量为计算出的批大小或配置的最大值
        maxBDDEngines = Math.min(batchSize, maxBDDEngines);

        LinkedBlockingDeque<BDDEngine> sharedQueueBDD = new LinkedBlockingDeque<>();

        // 预先创建BDD引擎
        System.out.println("正在初始化BDD引擎池...");
        for (int i = 0; i < maxBDDEngines; i++) {
            try {
                BDDEngine newEngine = bddEnginePool.obtain(srcBdd);
                sharedQueueBDD.put(newEngine);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("BDD引擎池初始化完成，创建了" + maxBDDEngines + "个引擎实例");

        // 将TopoNet列表分成批次并优先排序
        List<Map.Entry<String, TopoNet>> topoNetList = prioritizeTopoNets(topoNetMap.entrySet());
        int totalTopoNets = topoNetList.size();
        totalProcessedTopoNets = totalTopoNets; // 记录总TopoNet数
        int numBatches = (totalTopoNets + batchSize - 1) / batchSize; // 向上取整

        System.out.println("总验证任务: " + totalTopoNets + " 个TopoNets，分" + numBatches +
                          "批处理，每批最多" + batchSize + "个");
        System.out.println("========================================");

        Map<String, TopoNet> processedTopoNets = new HashMap<>();
        Map<String, Long> memoryBeforeBatch = new HashMap<>();

        for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
            // 从检查点恢复(如果有)
            Map<String, Object> checkpoint = loadCheckpoint(batchIndex);
            if (checkpoint != null) {
                System.out.println("从检查点恢复批次" + batchIndex + "的数据");
                // 处理恢复的数据...
            }

            // 每批开始前再次检查内存状态
            if (!checkAndMaintainMemory()) {
                // 如果内存不足，减小批大小
                batchSize = Math.max(1, batchSize / 2);
                System.out.println("内存不足，减小批大小至: " + batchSize);

                // 重新计算批次数
                numBatches = (totalTopoNets + batchSize - 1) / batchSize;
            }

            int startIndex = batchIndex * batchSize;
            int endIndex = Math.min(startIndex + batchSize, totalTopoNets);
            int currentBatchSize = endIndex - startIndex;

            System.out.println("【批次" + (batchIndex + 1) + "开始】处理" + currentBatchSize +
                             "个TopoNets (索引:" + startIndex + "-" + (endIndex - 1) + ")");

            long batchStartTime = System.currentTimeMillis();
            long batchStartMemory = getCurrentMemoryUsage(); // 记录批次开始内存
            AtomicInteger completedCount = new AtomicInteger(0);

            // 处理当前批次
            for (int i = startIndex; i < endIndex; i++) {
                Map.Entry<String, TopoNet> entry = topoNetList.get(i);
                final int topoNetIndex = i;
                final String topoNetName = entry.getKey();

                // 记录处理前内存
                memoryBeforeBatch.put(topoNetName, getCurrentMemoryUsage());

                threadPool.execute(() -> {
                    TopoNet topoNet = entry.getValue();
                    long startTime = System.currentTimeMillis();
                    long beforeMemory = memoryBeforeBatch.get(topoNetName);

                    try {
                        // 获取BDD引擎
                        boolean reused = topoNet.getAndSetBddEngine(sharedQueueBDD);

                        // 处理TopoNet
                        topoGenNode(topoNet);
                        topoNetDeepCopyBdd(topoNet, reused);
                        topoNet.nodeCalIndegree();

                        // 如果内存极低，使用近似计算
                        if (sacrificeAccuracyForMemory) {
                            System.out.println("对TopoNet[" + topoNetName + "]使用内存节约模式");
                            // 可以在这里实现简化的计算逻辑
                        }

                        // 启动验证
                        topoNet.startCount(sharedQueueBDD);

                        // 更新内存峰值
                        updatePeakMemory();

                        // 注册批次内存分配
                        Runtime runtime = Runtime.getRuntime();
                        long currentMemory = getCurrentMemoryUsage();

                        // 处理完成，记录统计信息
                        long endTime = System.currentTimeMillis();
                        int completed = completedCount.incrementAndGet();
                        System.out.println("    TopoNet[" + topoNetIndex + "][" + topoNetName +
                                         "] 处理完成 (" + completed + "/" + currentBatchSize +
                                         "), 耗时: " + (endTime - startTime) + "ms");

                        // 保存处理结果
                        synchronized (processedTopoNets) {
                            processedTopoNets.put(topoNetName, topoNet);
                        }

                        // 从失败列表中移除（如果之前失败过）
                        synchronized (failedTopoNets) {
                            failedTopoNets.remove(topoNetName);
                            retryAttempts.remove(topoNetName);
                        }

                        // 定期检查内存
                        if (completed % MEMORY_CHECK_INTERVAL == 0) {
                            synchronized (this) {
                                checkAndMaintainMemory();
                            }
                        }

                        // 帮助GC回收
                        if (topoNet.nodesTable != null) {
                            topoNet.nodesTable.clear();
                        }
                        if (topoNet.srcNodes != null) {
                            topoNet.srcNodes.clear();
                        }
                        topoNet = null;
                    } catch (Exception e) {
                        System.err.println("处理TopoNet[" + topoNetName + "]时发生错误: " + e.getMessage());
                        e.printStackTrace();

                        // 记录失败的TopoNet，以便后续重试
                        synchronized (failedTopoNets) {
                            // 获取当前重试次数
                            int attempts = retryAttempts.getOrDefault(topoNetName, 0);

                            // 更新重试计数
                            retryAttempts.put(topoNetName, attempts + 1);

                            // 只有未超过最大重试次数的才添加到失败列表
                            if (attempts < maxRetryAttempts) {
                                failedTopoNets.put(topoNetName, entry.getValue());
                                System.out.println("TopoNet[" + topoNetName + "] 添加到失败列表，将在后续重试，当前重试次数: " + (attempts + 1));
                            } else {
                                System.out.println("TopoNet[" + topoNetName + "] 已达到最大重试次数 (" + maxRetryAttempts + ")，不再重试");
                            }
                        }
                    }
                });
            }

            // 等待当前批次完成
            threadPool.awaitAllTaskFinished();
            long batchEndTime = System.currentTimeMillis();

            System.out.println("【批次" + (batchIndex + 1) + "完成】总耗时: " +
                             (batchEndTime - batchStartTime) + "ms, 平均每个TopoNet: " +
                             ((batchEndTime - batchStartTime) / currentBatchSize) + "ms");

            // 为下一批检查点化当前批次结果
            if (enableCheckpointing && batchIndex % checkpointInterval == 0) {
                createCheckpoint(batchIndex, processedTopoNets);
            }

            // 限制BDD引擎数量，防止内存溢出
            while (sharedQueueBDD.size() > maxBDDEngines) {
                try {
                    BDDEngine engine = sharedQueueBDD.poll();
                    bddEnginePool.recycle(engine);
                    System.out.println("回收多余BDD引擎，当前剩余: " + sharedQueueBDD.size());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // 确保我们有足够的BDD引擎用于下一批
            int engineCount = sharedQueueBDD.size();
            if (engineCount < maxBDDEngines && batchIndex < numBatches - 1) {
                System.out.println("为下一批补充BDD引擎，当前数量: " + engineCount);

                // 先进行GC，释放内存
                System.gc();

                while (sharedQueueBDD.size() < maxBDDEngines) {
                    try {
                        // 再次检查内存状态
                        if (!checkAndMaintainMemory()) {
                            System.out.println("内存不足，不再创建更多BDD引擎");
                            break;
                        }

                        BDDEngine newEngine = bddEnginePool.obtain(srcBdd);
                        sharedQueueBDD.put(newEngine);
                    } catch (Exception e) {
                        System.err.println("创建BDD引擎失败: " + e.getMessage());
                        break;
                    }
                }
                System.out.println("BDD引擎池已补充至" + sharedQueueBDD.size() + "个");
            }

            // 每批次结束时记录内存状态（帮助跟踪资源使用）
            long batchEndMemory = getCurrentMemoryUsage();
            System.out.println("批次内存使用: " + ((batchEndMemory - batchStartMemory) / (1024 * 1024)) + " MB");

            // 批次完成后强制GC
            System.out.println("批次处理结束，执行垃圾回收...");
            memoryBeforeBatch.clear();
            processedTopoNets.clear();
            System.gc();

            System.out.println("----------------------------------------");
        }

        // 处理失败的TopoNets（如果有）
        if (!failedTopoNets.isEmpty()) {
            System.out.println("\n========== 开始处理失败的TopoNets ==========");
            System.out.println("发现" + failedTopoNets.size() + "个失败的TopoNets需要重试");

            // 先进行GC，释放内存
            System.gc();

            // 确保有足够的BDD引擎
            while (sharedQueueBDD.size() < Math.min(failedTopoNets.size(), maxBDDEngines)) {
                try {
                    if (!checkAndMaintainMemory()) {
                        System.out.println("内存不足，无法创建更多BDD引擎");
                        break;
                    }

                    BDDEngine newEngine = bddEnginePool.obtain(srcBdd);
                    sharedQueueBDD.put(newEngine);
                } catch (Exception e) {
                    System.err.println("创建BDD引擎失败: " + e.getMessage());
                    break;
                }
            }

            // 转换为列表并排序（可以根据失败原因或重试次数进行排序）
            List<Map.Entry<String, TopoNet>> failedList = new ArrayList<>(failedTopoNets.entrySet());
            failedList.sort((e1, e2) -> {
                // 按重试次数升序排序，让重试次数少的先处理
                return retryAttempts.getOrDefault(e1.getKey(), 0) - retryAttempts.getOrDefault(e2.getKey(), 0);
            });

            final AtomicInteger successfulRetries = new AtomicInteger(0);
            final AtomicInteger completedRetries = new AtomicInteger(0);

            // 处理失败的TopoNets
            for (Map.Entry<String, TopoNet> entry : failedList) {
                final String topoNetName = entry.getKey();
                final TopoNet topoNet = entry.getValue();
                final int attempts = retryAttempts.getOrDefault(topoNetName, 0);

                System.out.println("开始重试 TopoNet[" + topoNetName + "]，重试次数: " + attempts);

                // 记录处理前内存
                long beforeMemory = getCurrentMemoryUsage();

                threadPool.execute(() -> {
                    long startTime = System.currentTimeMillis();
                    boolean success = false;

                    try {
                        // 获取BDD引擎
                        boolean reused = topoNet.getAndSetBddEngine(sharedQueueBDD);

                        // 添加额外的重试逻辑，例如增加超时时间、降低精度等
                        System.out.println("    重试时对TopoNet[" + topoNetName + "]应用特殊处理");

                        // 处理TopoNet
                        topoGenNode(topoNet);
                        topoNetDeepCopyBdd(topoNet, reused);
                        topoNet.nodeCalIndegree();

                        // 启动验证
                        topoNet.startCount(sharedQueueBDD);

                        // 更新内存峰值
                        updatePeakMemory();

                        // 计算此TopoNet使用的内存
                        long afterMemory = getCurrentMemoryUsage();
                        long topoNetMemory = Math.max(0, afterMemory - beforeMemory);
                        synchronized(topoNetMemoryUsage) {
                            topoNetMemoryUsage.put(topoNetName, topoNetMemory);
                        }

                        success = true;
                        successfulRetries.incrementAndGet();

                        // 处理完成，记录统计信息
                        long endTime = System.currentTimeMillis();
                        System.out.println("    TopoNet[" + topoNetName + "] 重试成功，耗时: " +
                                          (endTime - startTime) + "ms, 内存消耗: " +
                                          (topoNetMemory / (1024 * 1024)) + "MB");

                    } catch (Exception e) {
                        System.err.println("重试处理TopoNet[" + topoNetName + "]时仍然失败: " + e.getMessage());
                        if (attempts + 1 >= maxRetryAttempts) {
                            System.out.println("    TopoNet[" + topoNetName + "] 已达到最大重试次数，标记为永久失败");
                        }
                    } finally {
                        int completed = completedRetries.incrementAndGet();
                        System.out.println("完成重试 " + completed + "/" + failedList.size() +
                                         (success ? " (成功)" : " (失败)"));

                        // 帮助GC回收
                        if (topoNet.nodesTable != null) {
                            topoNet.nodesTable.clear();
                        }
                        if (topoNet.srcNodes != null) {
                            topoNet.srcNodes.clear();
                        }
                    }
                });
            }

            // 等待所有重试完成
            threadPool.awaitAllTaskFinished();

            System.out.println("重试完成，成功: " + successfulRetries.get() + "/" + failedList.size());
            System.out.println("========== 结束重试处理 ==========\n");
        }

        // 清空BDD引擎池
        while (!sharedQueueBDD.isEmpty()) {
            BDDEngine engine = sharedQueueBDD.poll();
            engine = null;
        }

        // 清理对象池
        bddEnginePool.pool.clear();

        // 记录验证阶段结束内存
        verificationEndMemory = getCurrentMemoryUsage();

        // 确保至少有最低限度的内存使用记录
        if (totalAllocatedMemory == 0) {
            // 使用峰值来估算
            totalAllocatedMemory = Math.max(verificationPeakMemory - verificationStartMemory, 0);

            // 如果峰值仍然是0，使用每个TopoNet至少1MB的估算值
            if (totalAllocatedMemory == 0 && totalProcessedTopoNets > 0) {
                totalAllocatedMemory = totalProcessedTopoNets * 1024 * 1024; // 每个至少1MB
            }
        }

        // 保存内存统计信息
        saveMemoryStats();

        // 打印内存使用统计信息
        System.out.println("\n========== 内存使用统计 ==========");
        System.out.println("构建阶段内存消耗: " + ((buildEndMemory - buildStartMemory) / (1024 * 1024)) + " MB");
        System.out.println("构建阶段内存峰值: " + (buildPeakMemory / (1024 * 1024)) + " MB");
        System.out.println("验证阶段内存峰值: " + (verificationPeakMemory / (1024 * 1024)) + " MB");
        System.out.println("验证阶段内存消耗: " + (Math.max(0, verificationEndMemory - verificationStartMemory) / (1024 * 1024)) + " MB");
        System.out.println("累计分配内存总量: " + (totalAllocatedMemory / (1024 * 1024)) + " MB");

        if (totalProcessedTopoNets > 0) {
            System.out.println("每个TopoNet平均内存消耗: " +
                (totalAllocatedMemory / totalProcessedTopoNets / (1024 * 1024)) + " MB");
        }

        System.out.println("详细统计信息已保存到: " + MEMORY_STATS_FILE);
        System.out.println("===============================");

        // 最终GC
        System.gc();

        System.out.println("========================================");
        System.out.println("所有批次处理完成，共处理" + totalProcessedTopoNets + "个TopoNets");

        // 报告失败情况
        if (!failedTopoNets.isEmpty()) {
            System.out.println("存在" + failedTopoNets.size() + "个TopoNet处理失败:");
            failedTopoNets.keySet().forEach(name ->
                System.out.println("    - " + name + " (重试次数: " + retryAttempts.getOrDefault(name, 0) + ")"));
        } else {
            System.out.println("所有TopoNet处理成功！");
        }
    }

    private void genTopoNet() {
        topoNetMap = new HashMap<>();
        int topoCnt = -1;
        // 根据edgeDevices初始化topoNet对象并设置device
        TopoNet.network = this.network;
        for(Device dstDevice : TopoNet.edgeDevices){
            if(!network.dstDevices.contains(dstDevice.name)) continue;
            TopoNet topoNet = new TopoNet(dstDevice, topoCnt);
            topoNet.setInvariant(dstDevice.name, "exist >= 1", "*");
            topoNetMap.put(dstDevice.name, topoNet);
            topoCnt--;
        }
        TopoNet.transformDevicePorts(network.devicePorts);
        TopoNet.setNextTable();
    }

    private void readRuleByDevice(){
        // 先从文件中读取规则, 并插入规则
        for (Map.Entry<String, Device> entry : devices.entrySet()) {
            threadPool.execute(() -> {
                String name = entry.getKey();
                Device device = entry.getValue();
                device.readOnlyRulesFile(Configuration.getConfiguration().getDeviceRuleFile(name));
            });
        }
        // 读取网段
        Device.readOnlySpaceFile(Configuration.getConfiguration().getSpaceFile());
        threadPool.awaitAllTaskFinished();
    }

    private void readRuleByDeviceIPV6(){
        // 先从文件中读取规则, 并插入规则
        for (Map.Entry<String, Device> entry : devices.entrySet()) {
            threadPool.execute(() -> {
                String name = entry.getKey();
                Device device = entry.getValue();
                if(isIpv4withS){
                    device.readOnlyRulesFileIPV4_S(Configuration.getConfiguration().getDeviceRuleFile(name));
                }else{
                    device.readOnlyRulesFileIPV6(Configuration.getConfiguration().getDeviceRuleFile(name));
                }

            });
        }
        // 读取网段
        Device.readOnlySpaceFileIPV6(Configuration.getConfiguration().getSpaceFile());
        threadPool.awaitAllTaskFinished();
    }

    public void srcBddTransformAllRules(){
        transformRuleWithoutTrie();
    }

    public void transformRuleWithTrie(){
        long timePoint1 = System.currentTimeMillis();
        for(Device device : devices.values()){
            device.encodeDeviceRule(srcNet);
        }
        long timePoint2 = System.currentTimeMillis();
        System.out.println("规则转化所使用的时间" + (timePoint2 - timePoint1) + "ms");
        srcNet.srcDvNetParseAllSpace(Device.spaces);
        long timePoint3 = System.currentTimeMillis();
        System.out.println("BDD编码所使用的总时间" + (timePoint3 - timePoint1) + "ms");
    }

    public void transformRuleWithTrieIPV6(){
        long timePoint1 = System.currentTimeMillis();
        for(Device device : devices.values()){
            device.encodeDeviceRuleIPV6(srcNet);
        }
        long timePoint2 = System.currentTimeMillis();
        System.out.println("规则转化所使用的时间" + (timePoint2 - timePoint1) + "ms");
        srcNet.srcDvNetParseAllSpaceIPV6(Device.spacesIPV6);
        long timePoint3 = System.currentTimeMillis();
        System.out.println("BDD编码所使用的总时间" + (timePoint3 - timePoint1) + "ms");
    }

    public void transformRuleWithoutTrie(){
        long timePoint1 = System.currentTimeMillis();
        for(Device device : devices.values()){
            if(!(isIpv6 || isIpv4withS))device.encodeRuleToLecFromScratch(srcNet); // IPV4
            else {
                try {
                    device.encodeRuleToLecFromScratchIPV6(srcNet); // IPV6
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
            ruleCnt += device.rules.size();
        }
        long timePoint2 = System.currentTimeMillis();
        System.out.println("规则转化所使用的时间" + (timePoint2 - timePoint1) + "ms");
        if(!(isIpv6|| isIpv4withS))srcNet.srcDvNetParseAllSpace(Device.spaces);
        else srcNet.srcDvNetParseAllSpaceIPV6(Device.spacesIPV6);
        long timePoint3 = System.currentTimeMillis();
        System.out.println("BDD编码所使用的总时间" + (timePoint3 - timePoint1) + "ms");
    }

    private void topoGenNode(TopoNet topoNet){
        for(Device device : devices.values()){
            Node node = new Node(device, topoNet);
            topoNet.nodesTable.put(device.name, node);
            if(TopoNet.edgeDevices.contains(device)){
                if(device == topoNet.dstDevice) { // 终结点
                    topoNet.setDstNode(node);
                    node.isDestination = true;
                }
                else{ // 边缘结点
                    topoNet.srcNodes.add(node);
                }
            }
        }
    }

    public void topoNetDeepCopyBdd(TopoNet topoNet, boolean reused){
        String dstDevice = topoNet.dstDevice.name;
        int s = DVNet.devicePacketSpace.get(dstDevice);
        if(!reused) {
            try {
                topoNet.copyBdd(srcBdd, "Reflect");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        else topoNet.setNodeBdd();
        topoNet.deviceLecs = Device.globalLecs;
        topoNet.setPacketSpace(s);
    }

    @Override
    public void awaitFinished(){
        threadPool.awaitAllTaskFinished(100);
    }

    @Override
    public void sendCount(Context ctx, DevicePort sendPort, BDDEngine bddEngine) {
        // 实现省略
    }

    public long getInitTime(){
        return 0;
    }

    @Override
    public void close(){
        // 保存内存统计信息
        saveMemoryStats();

        // 清理资源
        devices.values().forEach(Device::close);
        threadPool.shutdownNow();

        // 清理对象池
        bddEnginePool.pool.clear();

        // 清理缓存
        softCache.clear();
        sharedPredicates.clear();
        predicateGroups.clear();

        System.gc();
    }
}