package org.sngroup.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 性能监控服务，负责收集和报告系统性能指标
 */
public class MonitorService {
    private static final MonitorService INSTANCE = new MonitorService();
    
    // 内存监控
    private final Map<String, MemorySnapshot> memorySnapshots = new ConcurrentHashMap<>();
    
    // BDD性能监控
    private final AtomicInteger bddCreationCount = new AtomicInteger(0);
    private final AtomicInteger bddReuseCount = new AtomicInteger(0);
    private final AtomicLong bddWaitTime = new AtomicLong(0);
    private final AtomicLong bddUsageTime = new AtomicLong(0);
    
    // 批次监控
    private final Map<Integer, BatchMetrics> batchMetrics = new ConcurrentHashMap<>();
    private int currentBatch = 0;
    
    // 构建和验证阶段监控
    private long buildStartTime = 0;
    private long buildEndTime = 0;
    private long buildStartMemory = 0;
    private long buildEndMemory = 0;
    private long verifyStartTime = 0;
    private long verifyEndTime = 0;
    private long verifyStartMemory = 0;
    private long verifyEndMemory = 0;

    // GC监控
    private final List<GarbageCollectorMXBean> gcBeans;
    private long lastGcCount = 0;
    private long lastGcTime = 0;
    private long buildStartGcCount = 0;
    private long buildStartGcTime = 0;
    private long buildEndGcCount = 0;
    private long buildEndGcTime = 0;
    private long verifyStartGcCount = 0;
    private long verifyStartGcTime = 0;
    private long verifyEndGcCount = 0;
    private long verifyEndGcTime = 0;

    // 日志文件
    private String performanceLogFile = "./performance_metrics.csv";
    private String memoryLogFile = "./memory_usage.csv";
    private String topoNetLogFile = "./toponet_performance.csv";
    private String bddLogFile = "./bdd_performance.csv";
    private String summaryFile = "./performance_summary.txt";

    private MonitorService() {
        // 初始化GC监控
        gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        // 创建日志文件并写入表头
        try (PrintWriter writer = new PrintWriter(new FileWriter(performanceLogFile))) {
            writer.println("时间戳,事件,批次ID,TopoNetID,已用内存,空闲内存,总内存,GC次数,GC时间(ms),持续时间(ms),BDD节点数");
        } catch (IOException e) {
            System.err.println("无法创建性能日志文件: " + e.getMessage());
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(memoryLogFile))) {
            writer.println("时间戳,事件,已用内存(KB),空闲内存(KB),总内存(KB),最大内存(KB)");
        } catch (IOException e) {
            System.err.println("无法创建内存日志文件: " + e.getMessage());
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(topoNetLogFile))) {
            writer.println("批次ID,TopoNetID,初始化内存(KB),计算内存(KB),初始化时间(ms),计算时间(ms),BDD节点数,BDD复用");
        } catch (IOException e) {
            System.err.println("无法创建TopoNet日志文件: " + e.getMessage());
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(bddLogFile))) {
            writer.println("时间戳,事件,创建次数,复用次数,复用率,等待时间(ms),使用时间(ms)");
        } catch (IOException e) {
            System.err.println("无法创建BDD日志文件: " + e.getMessage());
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(summaryFile))) {
            writer.println("========== 性能监控摘要 ==========");
            writer.println("记录时间: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            writer.println();
        } catch (IOException e) {
            System.err.println("无法创建摘要文件: " + e.getMessage());
        }
    }

    public static MonitorService getInstance() {
        return INSTANCE;
    }

    /**
     * 开始构建阶段监控
     */
    public void startBuildPhase() {
        buildStartTime = System.currentTimeMillis();
        buildStartMemory = getUsedMemory();
        buildStartGcCount = getGCCount();
        buildStartGcTime = getGCTime();

        // 记录日志
        logMemoryUsage("BuildStart");
        logEvent("BuildStart", -1, "", getMemoryUsage(), getGCStats(), 0, 0);

        System.out.println("开始构建阶段监控");
    }

    /**
     * 结束构建阶段监控
     */
    public void endBuildPhase() {
        buildEndTime = System.currentTimeMillis();
        buildEndMemory = getUsedMemory();
        buildEndGcCount = getGCCount();
        buildEndGcTime = getGCTime();

        long duration = buildEndTime - buildStartTime;
        long memoryUsed = buildEndMemory - buildStartMemory;
        long gcCount = buildEndGcCount - buildStartGcCount;
        long gcTime = buildEndGcTime - buildStartGcTime;

        // 记录日志
        logMemoryUsage("BuildEnd");
        logEvent("BuildEnd", -1, "", getMemoryUsage(), getGCStats(), duration, 0);

        // 输出统计信息
        System.out.println("\n===== 构建阶段性能统计 =====");
        System.out.println("总时间: " + duration + "ms");
        System.out.println("内存使用: " + (memoryUsed / 1024) + "KB");
        System.out.println("GC次数: " + gcCount);
        System.out.println("GC总时间: " + gcTime + "ms");

        // 写入摘要文件
        try (PrintWriter writer = new PrintWriter(new FileWriter(summaryFile, true))) {
            writer.println("----- 构建阶段性能 -----");
            writer.println("总时间: " + duration + "ms");
            writer.println("内存使用: " + (memoryUsed / 1024) + "KB");
            writer.println("GC次数: " + gcCount);
            writer.println("GC总时间: " + gcTime + "ms");
            writer.println();
        } catch (IOException e) {
            System.err.println("写入摘要文件失败: " + e.getMessage());
        }
    }

    /**
     * 开始验证阶段监控
     */
    public void startVerifyPhase() {
        verifyStartTime = System.currentTimeMillis();
        verifyStartMemory = getUsedMemory();
        verifyStartGcCount = getGCCount();
        verifyStartGcTime = getGCTime();

        // 记录日志
        logMemoryUsage("VerifyStart");
        logEvent("VerifyStart", -1, "", getMemoryUsage(), getGCStats(), 0, 0);

        System.out.println("开始验证阶段监控");
    }

    /**
     * 结束验证阶段监控
     */
    public void endVerifyPhase() {
        verifyEndTime = System.currentTimeMillis();
        verifyEndMemory = getUsedMemory();
        verifyEndGcCount = getGCCount();
        verifyEndGcTime = getGCTime();

        long duration = verifyEndTime - verifyStartTime;
        long memoryUsed = verifyEndMemory - verifyStartMemory;
        long gcCount = verifyEndGcCount - verifyStartGcCount;
        long gcTime = verifyEndGcTime - verifyStartGcTime;

        // 记录日志
        logMemoryUsage("VerifyEnd");
        logEvent("VerifyEnd", -1, "", getMemoryUsage(), getGCStats(), duration, 0);

        // 输出统计信息
        System.out.println("\n===== 验证阶段性能统计 =====");
        System.out.println("总时间: " + duration + "ms");
        System.out.println("内存使用: " + (memoryUsed / 1024) + "KB");
        System.out.println("GC次数: " + gcCount);
        System.out.println("GC总时间: " + gcTime + "ms");

        // 写入摘要文件
        try (PrintWriter writer = new PrintWriter(new FileWriter(summaryFile, true))) {
            writer.println("----- 验证阶段性能 -----");
            writer.println("总时间: " + duration + "ms");
            writer.println("内存使用: " + (memoryUsed / 1024) + "KB");
            writer.println("GC次数: " + gcCount);
            writer.println("GC总时间: " + gcTime + "ms");
            writer.println();
        } catch (IOException e) {
            System.err.println("写入摘要文件失败: " + e.getMessage());
        }
    }

    /**
     * 初始化批次监控
     */
    public synchronized void startBatchMonitoring(int batchId, int batchSize) {
        currentBatch = batchId;
        BatchMetrics metrics = new BatchMetrics(batchId, batchSize);
        batchMetrics.put(batchId, metrics);

        // 记录批次开始状态
        logEvent("BatchStart", batchId, "", getMemoryUsage(), getGCStats(), 0, 0);
        logMemoryUsage("BatchStart_" + batchId);
        logBDDStats("BatchStart_" + batchId);

        System.out.println("开始批次 #" + batchId + " 监控 (大小: " + batchSize + ")");
    }

    /**
     * 完成批次监控
     */
    public synchronized void endBatchMonitoring(int batchId) {
        BatchMetrics metrics = batchMetrics.get(batchId);
        if (metrics != null) {
            metrics.endTime = System.currentTimeMillis();

            // 记录批次结束状态
            long duration = metrics.endTime - metrics.startTime;
            logEvent("BatchEnd", batchId, "", getMemoryUsage(), getGCStats(), duration, 0);
            logMemoryUsage("BatchEnd_" + batchId);
            logBDDStats("BatchEnd_" + batchId);

            // 计算批次统计信息
            long gcCountDiff = getGCCount() - metrics.startGcCount;
            long gcTimeDiff = getGCTime() - metrics.startGcTime;
            double reuseRate = 0;
            if (metrics.reuseCount + metrics.createCount > 0) {
                reuseRate = (double)metrics.reuseCount / (metrics.reuseCount + metrics.createCount) * 100.0;
            }

            long avgInitMemory = metrics.getAverageInitMemory();
            long avgComputeMemory = metrics.getAverageComputeMemory();
            long maxInitMemory = metrics.getMaxInitMemory();
            long maxComputeMemory = metrics.getMaxComputeMemory();

            // 输出批次统计信息
            System.out.println("\n==== 批次 #" + batchId + " 性能统计 ====");
            System.out.println("处理时间: " + duration + "ms");
            System.out.println("TopoNet数量: " + metrics.topoNetIds.size());
            System.out.println("初始化内存列表大小: " + metrics.initMemoryList.size());
            System.out.println("计算内存列表大小: " + metrics.computeMemoryList.size());
            System.out.println("平均每个TopoNet初始化内存: " + avgInitMemory + "KB");
            System.out.println("平均每个TopoNet计算内存: " + avgComputeMemory + "KB");
            System.out.println("最大TopoNet初始化内存: " + maxInitMemory + "KB");
            System.out.println("最大TopoNet计算内存: " + maxComputeMemory + "KB");
            System.out.println("GC次数: " + gcCountDiff);
            System.out.println("GC总时间: " + gcTimeDiff + "ms");
            System.out.println("BDD复用次数: " + metrics.reuseCount);
            System.out.println("BDD创建次数: " + metrics.createCount);
            System.out.println("BDD复用率: " + String.format("%.2f", reuseRate) + "%");

            // 写入摘要文件
            try (PrintWriter writer = new PrintWriter(new FileWriter(summaryFile, true))) {
                writer.println("----- 批次 #" + batchId + " 性能统计 -----");
                writer.println("处理时间: " + duration + "ms");
                writer.println("TopoNet数量: " + metrics.topoNetIds.size());
                writer.println("平均每个TopoNet初始化内存: " + avgInitMemory + "KB");
                writer.println("平均每个TopoNet计算内存: " + avgComputeMemory + "KB");
                writer.println("最大TopoNet初始化内存: " + maxInitMemory + "KB");
                writer.println("最大TopoNet计算内存: " + maxComputeMemory + "KB");
                writer.println("GC次数: " + gcCountDiff);
                writer.println("GC总时间: " + gcTimeDiff + "ms");
                writer.println("BDD复用次数: " + metrics.reuseCount);
                writer.println("BDD创建次数: " + metrics.createCount);
                writer.println("BDD复用率: " + String.format("%.2f", reuseRate) + "%");
                writer.println();
            } catch (IOException e) {
                System.err.println("写入摘要文件失败: " + e.getMessage());
            }
        } else {
            System.err.println("警告: 批次 #" + batchId + " 的指标数据不存在");
        }
    }

    /**
     * 开始TopoNet初始化监控
     */
    public synchronized void startTopoNetInitialization(String topoNetId) {
        MemorySnapshot snapshot = new MemorySnapshot();
        snapshot.startMemory = getUsedMemory();
        snapshot.startTime = System.currentTimeMillis();
        snapshot.startGcCount = getGCCount();
        snapshot.startGcTime = getGCTime();

        memorySnapshots.put(topoNetId, snapshot);

        // 记录初始化开始
        logEvent("TopoNetInitStart", currentBatch, topoNetId, getMemoryUsage(), getGCStats(), 0, 0);
    }

    /**
     * 结束TopoNet初始化监控
     */
    public synchronized void endTopoNetInitialization(String topoNetId, int bddNodesCount, boolean reused) {
        MemorySnapshot snapshot = memorySnapshots.get(topoNetId);
        if (snapshot != null) {
            snapshot.initEndMemory = getUsedMemory();
            snapshot.initEndTime = System.currentTimeMillis();
            long duration = snapshot.initEndTime - snapshot.startTime;
            long memoryUsed = snapshot.initEndMemory - snapshot.startMemory;

            // 更新批次统计信息
            BatchMetrics metrics = batchMetrics.get(currentBatch);
            if (metrics != null) {
                metrics.addTopoNetId(topoNetId);
                metrics.addInitMemory(memoryUsed);
                metrics.addInitTime(duration);
                metrics.addBddNodes(bddNodesCount);
                if (reused) {
                    metrics.reuseCount++;
                    bddReuseCount.incrementAndGet();
                } else {
                    metrics.createCount++;
                    bddCreationCount.incrementAndGet();
                }
            }

            // 记录初始化结束
            logEvent("TopoNetInitEnd", currentBatch, topoNetId, getMemoryUsage(),
                    getGCStats(), duration, bddNodesCount);

            // 日志输出
            System.out.printf("TopoNet %s 初始化: %dms, 内存: %dKB, BDD节点: %d, %s\n",
                    topoNetId, duration, memoryUsed/1024, bddNodesCount,
                    reused ? "复用BDD" : "新建BDD");
        } else {
            System.err.println("警告: TopoNet " + topoNetId + " 的初始化信息不存在");
        }
    }

    /**
     * 开始TopoNet计算监控
     */
    public synchronized void startTopoNetComputation(String topoNetId) {
        MemorySnapshot snapshot = memorySnapshots.get(topoNetId);
        if (snapshot != null) {
            snapshot.computeStartMemory = getUsedMemory();
            snapshot.computeStartTime = System.currentTimeMillis();

            // 记录计算开始
            logEvent("TopoNetComputeStart", currentBatch, topoNetId,
                    getMemoryUsage(), getGCStats(), 0, 0);
        } else {
            System.err.println("警告: TopoNet " + topoNetId + " 的内存快照不存在");
        }
    }

    /**
     * 结束TopoNet计算监控
     */
    public synchronized void endTopoNetComputation(String topoNetId, int bddNodesCount) {
        MemorySnapshot snapshot = memorySnapshots.get(topoNetId);
        if (snapshot != null) {
            snapshot.computeEndMemory = getUsedMemory();
            snapshot.computeEndTime = System.currentTimeMillis();
            long duration = snapshot.computeEndTime - snapshot.computeStartTime;
            long memoryUsed = snapshot.computeEndMemory - snapshot.computeStartMemory;

            // 更新批次统计信息
            BatchMetrics metrics = batchMetrics.get(currentBatch);
            if (metrics != null) {
                metrics.addComputeMemory(memoryUsed);
                metrics.addComputeTime(duration);
            }

            // 记录计算结束
            logEvent("TopoNetComputeEnd", currentBatch, topoNetId,
                    getMemoryUsage(), getGCStats(), duration, bddNodesCount);

            // 记录TopoNet完整性能到日志
            logTopoNetPerformance(topoNetId, snapshot, bddNodesCount);

            System.out.printf("TopoNet %s 计算: %dms, 内存: %dKB, BDD节点: %d\n",
                    topoNetId, duration, memoryUsed/1024, bddNodesCount);

            // 清理不再需要的快照
            memorySnapshots.remove(topoNetId);
        } else {
            System.err.println("警告: TopoNet " + topoNetId + " 的内存快照不存在");
        }
    }

    /**
     * 记录BDD等待时间
     */
    public void recordBDDWaitTime(long waitTimeMs) {
        bddWaitTime.addAndGet(waitTimeMs);
    }

    /**
     * 记录BDD使用时间
     */
    public void recordBDDUsageTime(long usageTimeMs) {
        bddUsageTime.addAndGet(usageTimeMs);
    }

    /**
     * 获取当前内存使用情况
     */
    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * 获取内存使用指标
     */
    private String getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long freeMemory = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();
        return usedMemory + "," + freeMemory + "," + totalMemory;
    }

    /**
     * 获取当前GC统计
     */
    private String getGCStats() {
        long gcCount = getGCCount();
        long gcTime = getGCTime();
        return gcCount + "," + gcTime;
    }

    /**
     * 获取GC执行次数
     */
    private long getGCCount() {
        long gcCount = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            long count = gcBean.getCollectionCount();
            if (count != -1) {
                gcCount += count;
            }
        }
        return gcCount;
    }

    /**
     * 获取GC执行时间
     */
    private long getGCTime() {
        long gcTime = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            long time = gcBean.getCollectionTime();
            if (time != -1) {
                gcTime += time;
            }
        }
        return gcTime;
    }

    /**
     * 记录性能事件到日志文件
     */
    private synchronized void logEvent(String event, int batchId, String topoNetId,
                                      String memoryUsage, String gcStats,
                                      long duration, int bddNodesCount) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(performanceLogFile, true))) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            writer.println(sdf.format(new Date()) + "," + event + "," + batchId + "," +
                          topoNetId + "," + memoryUsage + "," + gcStats + "," +
                          duration + "," + bddNodesCount);
        } catch (IOException e) {
            System.err.println("写入性能日志失败: " + e.getMessage());
        }
    }

    /**
     * 记录内存使用情况到日志文件
     */
    private synchronized void logMemoryUsage(String event) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(memoryLogFile, true))) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            long freeMemory = runtime.freeMemory();
            long totalMemory = runtime.totalMemory();
            long maxMemory = runtime.maxMemory();

            writer.println(sdf.format(new Date()) + "," + event + "," +
                          (usedMemory / 1024) + "," + (freeMemory / 1024) + "," +
                          (totalMemory / 1024) + "," + (maxMemory / 1024));
        } catch (IOException e) {
            System.err.println("写入内存日志失败: " + e.getMessage());
        }
    }

    /**
     * 记录BDD统计到日志文件
     */
    private synchronized void logBDDStats(String event) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(bddLogFile, true))) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            int createCount = bddCreationCount.get();
            int reuseCount = bddReuseCount.get();
            double reuseRate = 0.0;
            if (createCount + reuseCount > 0) {
                reuseRate = (double)reuseCount / (createCount + reuseCount) * 100.0;
            }
            long waitTime = bddWaitTime.get();
            long usageTime = bddUsageTime.get();

            writer.println(sdf.format(new Date()) + "," + event + "," + createCount + "," +
                          reuseCount + "," + String.format("%.2f", reuseRate) + "," +
                          waitTime + "," + usageTime);
        } catch (IOException e) {
            System.err.println("写入BDD日志失败: " + e.getMessage());
        }
    }

    /**
     * 记录TopoNet性能数据到日志文件
     */
    private synchronized void logTopoNetPerformance(String topoNetId, MemorySnapshot snapshot, int bddNodesCount) {
        if (snapshot == null) {
            System.err.println("无法记录TopoNet " + topoNetId + " 的性能数据，内存快照为空");
            return;
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(topoNetLogFile, true))) {
            long initMemory = snapshot.initEndMemory - snapshot.startMemory;
            long computeMemory = snapshot.computeEndMemory - snapshot.computeStartMemory;
            long initTime = snapshot.initEndTime - snapshot.startTime;
            long computeTime = snapshot.computeEndTime - snapshot.computeStartTime;

            writer.println(currentBatch + "," + topoNetId + "," +
                          (initMemory / 1024) + "," + (computeMemory / 1024) + "," +
                          initTime + "," + computeTime + "," + bddNodesCount + "," +
                          (memorySnapshots.get(topoNetId) != null ? "复用" : "新建"));
        } catch (IOException e) {
            System.err.println("写入TopoNet日志失败: " + e.getMessage());
        }
    }

    /**
     * 输出性能总结报告
     */
    public void printSummary() {
        System.out.println("\n======== 性能监控总结 ========");
        System.out.println("BDD引擎创建次数: " + bddCreationCount.get());
        System.out.println("BDD引擎复用次数: " + bddReuseCount.get());

        int totalBddOps = bddCreationCount.get() + bddReuseCount.get();
        if (totalBddOps > 0) {
            double reuseRate = (double)bddReuseCount.get() / totalBddOps * 100;
            System.out.printf("BDD引擎复用率: %.2f%%\n", reuseRate);
        }

        System.out.println("BDD引擎等待总时间: " + bddWaitTime.get() + "ms");
        System.out.println("BDD引擎使用总时间: " + bddUsageTime.get() + "ms");
        System.out.println("总GC次数: " + getGCCount());
        System.out.println("总GC时间: " + getGCTime() + "ms");

        // 计算构建和验证阶段的统计
        long buildDuration = buildEndTime - buildStartTime;
        long buildMemoryUsed = buildEndMemory - buildStartMemory;
        long buildGcCount = buildEndGcCount - buildStartGcCount;
        long buildGcTime = buildEndGcTime - buildStartGcTime;

        long verifyDuration = verifyEndTime - verifyStartTime;
        long verifyMemoryUsed = verifyEndMemory - verifyStartMemory;
        long verifyGcCount = verifyEndGcCount - verifyStartGcCount;
        long verifyGcTime = verifyEndGcTime - verifyStartGcTime;

        System.out.println("\n构建阶段: " + buildDuration + "ms, 内存: " +
                          (buildMemoryUsed / 1024) + "KB, GC: " + buildGcCount + "次, " +
                          buildGcTime + "ms");
        System.out.println("验证阶段: " + verifyDuration + "ms, 内存: " +
                          (verifyMemoryUsed / 1024) + "KB, GC: " + verifyGcCount + "次, " +
                          verifyGcTime + "ms");

        // 批次统计汇总
        System.out.println("\n批次统计:");
        for (BatchMetrics metrics : batchMetrics.values()) {
            try {
                long duration = metrics.endTime - metrics.startTime;
                System.out.printf("批次 #%d: %dms, TopoNet数量: %d, 平均初始化内存: %dKB, 平均计算内存: %dKB\n",
                                metrics.batchId, duration, metrics.topoNetIds.size(),
                                metrics.getAverageInitMemory(),
                                metrics.getAverageComputeMemory());
            } catch (Exception e) {
                System.err.println("处理批次 #" + metrics.batchId + " 统计时发生错误: " + e.getMessage());
            }
        }

        System.out.println("==============================");

        // 写入总结到摘要文件
        try (PrintWriter writer = new PrintWriter(new FileWriter(summaryFile, true))) {
            writer.println("\n======== 性能监控总结 ========");
            writer.println("BDD引擎创建次数: " + bddCreationCount.get());
            writer.println("BDD引擎复用次数: " + bddReuseCount.get());

            if (totalBddOps > 0) {
                double reuseRate = (double)bddReuseCount.get() / totalBddOps * 100;
                writer.printf("BDD引擎复用率: %.2f%%\n", reuseRate);
            }

            writer.println("BDD引擎等待总时间: " + bddWaitTime.get() + "ms");
            writer.println("BDD引擎使用总时间: " + bddUsageTime.get() + "ms");
            writer.println("总GC次数: " + getGCCount());
            writer.println("总GC时间: " + getGCTime() + "ms");

            writer.println("\n构建阶段: " + buildDuration + "ms, 内存: " +
                          (buildMemoryUsed / 1024) + "KB, GC: " + buildGcCount + "次, " +
                          buildGcTime + "ms");
            writer.println("验证阶段: " + verifyDuration + "ms, 内存: " +
                          (verifyMemoryUsed / 1024) + "KB, GC: " + verifyGcCount + "次, " +
                          verifyGcTime + "ms");

            writer.println("\n批次统计汇总:");
            for (BatchMetrics metrics : batchMetrics.values()) {
                try {
                    long duration = metrics.endTime - metrics.startTime;
                    writer.printf("批次 #%d: %dms, TopoNet数量: %d, 平均初始化内存: %dKB, 平均计算内存: %dKB\n",
                                metrics.batchId, duration, metrics.topoNetIds.size(),
                                metrics.getAverageInitMemory(),
                                metrics.getAverageComputeMemory());
                } catch (Exception e) {
                    writer.println("处理批次 #" + metrics.batchId + " 统计时发生错误");
                }
            }

            writer.println("\n==============================");
        } catch (IOException e) {
            System.err.println("写入摘要文件失败: " + e.getMessage());
        }
    }

    /**
     * 内存快照类，记录单个TopoNet的内存使用
     */
    private static class MemorySnapshot {
        long startTime;
        long startMemory;
        long startGcCount;
        long startGcTime;

        long initEndTime;
        long initEndMemory;

        long computeStartTime;
        long computeStartMemory;

        long computeEndTime;
        long computeEndMemory;
    }

    /**
     * 批次指标类，记录单个批次的性能数据
     */
    private static class BatchMetrics {
        final int batchId;
        final int batchSize;
        final long startTime;
        long endTime;

        final List<String> topoNetIds = new ArrayList<>();
        final List<Long> initMemoryList = new ArrayList<>();
        final List<Long> computeMemoryList = new ArrayList<>();
        final List<Long> initTimeList = new ArrayList<>();
        final List<Long> computeTimeList = new ArrayList<>();
        final List<Integer> bddNodesList = new ArrayList<>();

        int reuseCount = 0;
        int createCount = 0;

        long startGcCount;
        long startGcTime;

        BatchMetrics(int batchId, int batchSize) {
            this.batchId = batchId;
            this.batchSize = batchSize;
            this.startTime = System.currentTimeMillis();

            // 记录初始GC状态
            GarbageCollectorMXBean youngGC = null;
            GarbageCollectorMXBean oldGC = null;

            for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
                String name = bean.getName();
                if (name.contains("Young") || name.contains("G1 Young Generation")) {
                    youngGC = bean;
                } else if (name.contains("Old") || name.contains("G1 Old Generation")) {
                    oldGC = bean;
                }
            }

            startGcCount = 0;
            startGcTime = 0;

            if (youngGC != null) {
                startGcCount += youngGC.getCollectionCount();
                startGcTime += youngGC.getCollectionTime();
            }

            if (oldGC != null) {
                startGcCount += oldGC.getCollectionCount();
                startGcTime += oldGC.getCollectionTime();
            }
        }

        void addTopoNetId(String id) {
            if (id != null) {
                topoNetIds.add(id);
            }
        }

        void addInitMemory(long memory) {
            if (memory > 0) {
                initMemoryList.add(memory);
            }
        }

        void addComputeMemory(long memory) {
            if (memory > 0) {
                computeMemoryList.add(memory);
            }
        }

        void addInitTime(long time) {
            if (time > 0) {
                initTimeList.add(time);
            }
        }

        void addComputeTime(long time) {
            if (time > 0) {
                computeTimeList.add(time);
            }
        }

        void addBddNodes(int nodes) {
            if (nodes > 0) {
                bddNodesList.add(nodes);
            }
        }

        long getAverageInitMemory() {
            if (initMemoryList == null || initMemoryList.isEmpty()) {
                return 0;
            }

            long sum = 0;
            for (Long memory : initMemoryList) {
                if (memory != null) {
                    sum += memory;
                }
            }
            return sum / (initMemoryList.size() * 1024); // 转换为KB
        }

        long getAverageComputeMemory() {
            if (computeMemoryList == null || computeMemoryList.isEmpty()) {
                return 0;
            }

            long sum = 0;
            for (Long memory : computeMemoryList) {
                if (memory != null) {
                    sum += memory;
                }
            }
            return sum / (computeMemoryList.size() * 1024); // 转换为KB
        }

        long getMaxInitMemory() {
            if (initMemoryList == null || initMemoryList.isEmpty()) {
                return 0;
            }

            long max = 0;
            for (Long memory : initMemoryList) {
                if (memory != null && memory > max) {
                    max = memory;
                }
            }
            return max / 1024; // 转换为KB
        }

        long getMaxComputeMemory() {
            if (computeMemoryList == null || computeMemoryList.isEmpty()) {
                return 0;
            }

            long max = 0;
            for (Long memory : computeMemoryList) {
                if (memory != null && memory > max) {
                    max = memory;
                }
            }
            return max / 1024; // 转换为KB
        }
    }
}