package org.sngroup.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPool {
    protected ThreadPoolExecutor threadPool;
    final BlockingQueue<Future<?>> futures;
    boolean _isShutdown = false;

    // 任务计数器和监控指标
    private final AtomicInteger pendingTasks = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger failedTasks = new AtomicInteger(0);

    public ThreadPool(){
        futures = new LinkedBlockingQueue<>();
    }

    public static ThreadPool FixedThreadPool(int size){
        ThreadPool t = new ThreadPool();
        // 使用自定义线程工厂，提高线程命名和优先级
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "VerificationWorker-" + threadNumber.getAndIncrement());
                thread.setPriority(Thread.NORM_PRIORITY);
                return thread;
            }
        };

        // 使用有界队列防止OOM和自定义拒绝策略
        t.threadPool = new ThreadPoolExecutor(
            size, // 核心线程数
            size, // 最大线程数
            60, TimeUnit.SECONDS, // 空闲线程超时
            new LinkedBlockingQueue<>(10000), // 有界队列
            threadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略：由调用者线程执行
        );

        return t;
    }

    public void execute(Runnable command) {
        // 包装任务以跟踪执行状态
        Runnable trackedCommand = () -> {
            try {
                pendingTasks.incrementAndGet();
                command.run();
                completedTasks.incrementAndGet();
            } catch (Exception e) {
                failedTasks.incrementAndGet();
                System.err.println("任务执行失败: " + e.getMessage());
                e.printStackTrace();
            } finally {
                pendingTasks.decrementAndGet();
            }
        };

        Future<?> future = threadPool.submit(trackedCommand);
        futures.add(future);
    }

    public void awaitAllTaskFinished(){
        awaitAllTaskFinished(100);
    }

    public void awaitAllTaskFinished(int timeout){
        long startTime = System.currentTimeMillis();
        int taskCount = futures.size();

        while (!futures.isEmpty()) {
            Future<?> future;
            try {
                // 更长的超时时间，避免过早放弃
                future = futures.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("等待任务完成被中断");
                break;
            }

            if (future == null) {
                // 超时后打印进度信息而不是继续
                long elapsed = System.currentTimeMillis() - startTime;
                int remaining = futures.size();
                int completed = taskCount - remaining;
                System.out.println(String.format("等待任务完成: %d/%d 完成 (%.1f%%), 已用时间: %.1f秒",
                    completed, taskCount,
                    (completed * 100.0 / taskCount),
                    (elapsed / 1000.0)));
                continue;
            }

            try {
                // 添加超时参数，防止单个任务阻塞太久
                future.get(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("等待任务结果被中断");
                break;
            } catch (ExecutionException e) {
                System.err.println("任务执行出错: " + e.getCause().getMessage());
                e.getCause().printStackTrace();
            } catch (TimeoutException e) {
                System.err.println("任务执行超时，继续等待其他任务");
                // 不取消任务，让它继续执行
            }
        }

        // 打印任务完成统计
        //long totalTime = System.currentTimeMillis() - startTime;
        //System.out.println(String.format("批次完成: 共 %d 个任务, 耗时 %.2f 秒",
            //taskCount, totalTime / 1000.0));
        //System.out.println(String.format("完成: %d, 失败: %d",
            //completedTasks.get(), failedTasks.get()));
    }

    public void shutdownNow(){
        threadPool.shutdownNow();
        _isShutdown = true;
    }

    public boolean isShutdown(){
        return _isShutdown;
    }

    // 获取线程池指标
    public Map<String, Integer> getMetrics() {
        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("activeThreads", threadPool.getActiveCount());
        metrics.put("pendingTasks", pendingTasks.get());
        metrics.put("completedTasks", completedTasks.get());
        metrics.put("failedTasks", failedTasks.get());
        metrics.put("queueSize", threadPool.getQueue().size());
        return metrics;
    }
}