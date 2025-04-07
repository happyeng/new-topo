package org.sngroup.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 验证统计类，用于跟踪验证性能和进度
 */
public class VerificationStats {
    private final AtomicInteger completed = new AtomicInteger(0);
    private final AtomicInteger failed = new AtomicInteger(0);
    private final int total;
    private final long startTime;

    // 性能统计
    private final AtomicLong processingTime = new AtomicLong(0);
    private final AtomicLong nodeProcessed = new AtomicLong(0);

    /**
     * 创建验证统计对象
     * @param total 总验证任务数
     */
    public VerificationStats(int total) {
        this.total = total;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * 记录完成情况
     * @param success 是否成功
     */
    public void recordCompletion(boolean success) {
        if (success) {
            completed.incrementAndGet();
        } else {
            failed.incrementAndGet();
        }
    }

    /**
     * 记录处理时间和节点数
     * @param time 处理时间(毫秒)
     * @param nodes 处理的节点数
     */
    public void recordProcessing(long time, int nodes) {
        processingTime.addAndGet(time);
        nodeProcessed.addAndGet(nodes);
    }

    /**
     * 获取已完成任务数
     */
    public int getCompleted() {
        return completed.get();
    }

    /**
     * 获取失败任务数
     */
    public int getFailed() {
        return failed.get();
    }

    /**
     * 获取进度百分比
     */
    public int getProgressPercentage() {
        return total > 0 ? (completed.get() * 100 / total) : 0;
    }

    /**
     * 获取进度信息
     */
    public String getProgressInfo() {
        int done = completed.get();
        double rate = (double) done / ((System.currentTimeMillis() - startTime) / 1000.0);
        long remainingTime = rate > 0 ? (long) ((total - done) / rate) : 0;

        return String.format("进度: %d/%d (%d%%) 完成, %d 失败, %.2f 个/秒, 预计剩余时间: %s",
                            done, total, getProgressPercentage(), failed.get(), rate,
                            formatTime(remainingTime));
    }

    /**
     * 获取节点处理性能信息
     */
    public String getNodeProcessingInfo() {
        long nodes = nodeProcessed.get();
        long time = processingTime.get();

        if (nodes == 0 || time == 0) {
            return "尚无处理数据";
        }

        double nodesPerMs = (double) nodes / time;
        double nodesPerSec = nodesPerMs * 1000;

        return String.format("节点处理性能: %.2f 节点/秒 (共处理 %d 节点, 耗时 %d 毫秒)",
                            nodesPerSec, nodes, time);
    }

    /**
     * 将秒数格式化为时分秒
     */
    private String formatTime(long seconds) {
        if (seconds < 0) {
            return "00:00:00";
        }

        long hours = seconds / 3600;
        long minutes = (seconds % 3600) / 60;
        long secs = seconds % 60;

        return String.format("%02d:%02d:%02d", hours, minutes, secs);
    }
}