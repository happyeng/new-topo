package org.sngroup.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 实现工作窃取功能的线程池，更适合处理不均衡负载的任务
 */
public class WorkStealingThreadPool extends ThreadPool {
    private final Deque<Runnable>[] workQueues;
    private final Thread[] workers;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AtomicInteger pendingTasks = new AtomicInteger(0);
    private final Random random = new Random();

    /**
     * 创建具有工作窃取功能的线程池
     * @param numThreads 线程数量
     */
    @SuppressWarnings("unchecked")
    public WorkStealingThreadPool(int numThreads) {
        super();
        // 确保至少有一个线程
        numThreads = Math.max(1, numThreads);

        workQueues = new Deque[numThreads];
        workers = new Thread[numThreads];

        // 初始化工作队列
        for (int i = 0; i < numThreads; i++) {
            workQueues[i] = new ConcurrentLinkedDeque<>();
        }

        // 创建工作线程
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            workers[i] = new Thread(() -> workerLoop(threadId));
            workers[i].setName("WorkStealer-" + i);
            workers[i].start();
        }
    }

    /**
     * 工作线程的主循环
     */
    private void workerLoop(int threadId) {
        while (isRunning.get() || pendingTasks.get() > 0) {
            Runnable task = getTask(threadId);

            if (task != null) {
                try {
                    task.run();
                } catch (NullPointerException e) {
                    System.err.println("警告: 执行任务时发生空指针异常，可能是设备LEC未正确初始化");
                    e.printStackTrace();
                } catch (Exception e) {
                    System.err.println("警告: 执行任务时发生异常: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    pendingTasks.decrementAndGet();
                    
                    // 完成任务后从futures移除
                    synchronized (futures) {
                        if (!futures.isEmpty()) {
                            futures.poll();
                        }
                    }
                }
            } else {
                // 如果没有任务，短暂休眠避免CPU空转
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    if (!isRunning.get()) {
                        break;
                    }
                }
            }
        }
    }
    
    /**
     * 获取任务的策略：
     * 1. 首先尝试从自己的队列获取
     * 2. 如果自己队列为空，则尝试从其他队列窃取
     */
    private Runnable getTask(int threadId) {
        // 先从自己的队列取任务
        Runnable task = workQueues[threadId].pollFirst();
        if (task != null) {
            return task;
        }
        
        // 如果自己的队列为空，尝试窃取其他队列的任务
        return stealTask(threadId);
    }
    
    /**
     * 从其他队列窃取任务
     */
    private Runnable stealTask(int threadId) {
        // 随机选择起始队列索引
        int startIndex = random.nextInt(workQueues.length);
        
        // 尝试从其他队列窃取任务
        for (int i = 0; i < workQueues.length; i++) {
            int index = (startIndex + i) % workQueues.length;
            if (index != threadId && !workQueues[index].isEmpty()) {
                // 从队列尾部窃取任务，减少竞争
                Runnable stolenTask = workQueues[index].pollLast();
                if (stolenTask != null) {
                    return stolenTask;
                }
            }
        }
        
        return null;
    }
    
    /**
     * 提交任务到线程池
     */
    @Override
    public void execute(Runnable command) {
        if (!isRunning.get()) {
            throw new RejectedExecutionException("线程池已关闭");
        }
        
        // 生成一个Future并添加到futures列表
        Future<?> future = new FutureTask<>(command, null);
        futures.add(future);
        
        // 选择负载最小的队列
        int targetQueue = findLeastLoadedQueue();
        workQueues[targetQueue].addFirst(command);
        pendingTasks.incrementAndGet();
    }
    
    /**
     * 查找负载最小的队列
     */
    private int findLeastLoadedQueue() {
        int minSize = Integer.MAX_VALUE;
        int targetQueue = 0;
        
        for (int i = 0; i < workQueues.length; i++) {
            int size = workQueues[i].size();
            if (size < minSize) {
                minSize = size;
                targetQueue = i;
            }
        }
        
        return targetQueue;
    }
    
    /**
     * 关闭线程池
     */
    @Override
    public void shutdownNow() {
        isRunning.set(false);
        for (Thread worker : workers) {
            worker.interrupt();
        }
        _isShutdown = true;
    }
    
    /**
     * 等待所有任务完成
     */
    @Override
    public void awaitAllTaskFinished(int timeout) {
        while (pendingTasks.get() > 0) {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    /**
     * 获取当前活跃任务数
     */
    public int getActiveTaskCount() {
        return pendingTasks.get();
    }
    
    /**
     * 工厂方法，创建工作窃取线程池
     */
    public static WorkStealingThreadPool create(int size) {
        return new WorkStealingThreadPool(size);
    }
}