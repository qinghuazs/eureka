package com.netflix.discovery;

import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A supervisor task that schedules subtasks while enforce a timeout.
 * Wrapped subtasks must be thread safe.
 *
 * @author David Qiang Liu
 */
// 【定时任务监督器】Eureka 客户端两大定时任务（心跳、注册表刷新）的执行框架。
// 解决的问题：普通的 scheduleAtFixedRate 在任务超时/网络故障时会导致任务堆积。
// 核心设计 —— "自调度 + 超时监督 + 指数退避"：
//   1. 每次执行完（无论成败）都重新调度下一次，而不是固定频率
//   2. 子任务提交到独立线程池执行，监督线程用 future.get(timeout) 限时等待
//   3. 超时则把下次调度间隔翻倍（最大 maxDelay = timeout × expBackOffBound 倍），
//      网络恢复且执行成功后间隔立刻恢复为正常值
public class TimedSupervisorTask extends TimerTask {
    private static final Logger logger = LoggerFactory.getLogger(TimedSupervisorTask.class);

    private final Counter successCounter;
    private final Counter timeoutCounter;
    private final Counter rejectedCounter;
    private final Counter throwableCounter;
    private final LongGauge threadPoolLevelGauge;

    private final String name;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutor executor;
    private final long timeoutMillis;
    private final Runnable task;

    private final AtomicLong delay;
    private final long maxDelay;

    public TimedSupervisorTask(String name, ScheduledExecutorService scheduler, ThreadPoolExecutor executor,
                               int timeout, TimeUnit timeUnit, int expBackOffBound, Runnable task) {
        this.name = name;
        this.scheduler = scheduler;
        this.executor = executor;
        this.timeoutMillis = timeUnit.toMillis(timeout);
        this.task = task;
        this.delay = new AtomicLong(timeoutMillis);
        this.maxDelay = timeoutMillis * expBackOffBound;

        // Initialize the counters and register.
        successCounter = Monitors.newCounter("success");
        timeoutCounter = Monitors.newCounter("timeouts");
        rejectedCounter = Monitors.newCounter("rejectedExecutions");
        throwableCounter = Monitors.newCounter("throwables");
        threadPoolLevelGauge = new LongGauge(MonitorConfig.builder("threadPoolUsed").build());
        Monitors.registerObject(name, this);
    }

    @Override
    public void run() {
        Future<?> future = null;
        try {
            // 把真正的任务（HeartbeatThread / CacheRefreshThread）提交到工作线程池
            future = executor.submit(task);
            threadPoolLevelGauge.set((long) executor.getActiveCount());
            // 限时等待任务完成：超时即抛 TimeoutException（任务本身可能还在跑，会在 finally 中被 cancel）
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);  // block until done or timeout
            // 执行成功：把下次调度间隔重置回正常值（从退避状态恢复）
            delay.set(timeoutMillis);
            threadPoolLevelGauge.set((long) executor.getActiveCount());
            successCounter.increment();
        } catch (TimeoutException e) {
            logger.warn("task supervisor timed out", e);
            timeoutCounter.increment();

            // 超时：下次调度间隔翻倍（指数退避），但不超过 maxDelay 上限
            // 例：心跳 30s 超时 → 下次 60s → 120s → ... → 最大 300s
            long currentDelay = delay.get();
            long newDelay = Math.min(maxDelay, currentDelay * 2);
            delay.compareAndSet(currentDelay, newDelay);

        } catch (RejectedExecutionException e) {
            // 线程池拒绝（池满或已关闭）：只计数，不影响下次调度
            if (executor.isShutdown() || scheduler.isShutdown()) {
                logger.warn("task supervisor shutting down, reject the task", e);
            } else {
                logger.warn("task supervisor rejected the task", e);
            }

            rejectedCounter.increment();
        } catch (Throwable e) {
            // 任务抛出异常：只计数，定时循环不会因此中断
            if (executor.isShutdown() || scheduler.isShutdown()) {
                logger.warn("task supervisor shutting down, can't accept the task");
            } else {
                logger.warn("task supervisor threw an exception", e);
            }

            throwableCounter.increment();
        } finally {
            // 取消可能仍在执行的超时任务，防止任务堆积
            if (future != null) {
                future.cancel(true);
            }

            // 【自调度核心】无论成功/超时/异常，都调度下一次执行 —— 用 delay 当前值作为间隔
            // （这就是为什么超时退避能生效：delay 已被翻倍）
            if (!scheduler.isShutdown()) {
                scheduler.schedule(this, delay.get(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public boolean cancel() {
        Monitors.unregisterObject(name, this);
        return super.cancel();
    }
}