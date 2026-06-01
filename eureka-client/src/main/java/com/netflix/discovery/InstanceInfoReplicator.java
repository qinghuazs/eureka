package com.netflix.discovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A task for updating and replicating the local instanceinfo to the remote server. Properties of this task are:
 * - configured with a single update thread to guarantee sequential update to the remote server
 * - update tasks can be scheduled on-demand via onDemandUpdate()
 * - task processing is rate limited by burstSize
 * - a new update task is always scheduled automatically after an earlier update task. However if an on-demand task
 *   is started, the scheduled automatic update task is discarded (and a new one will be scheduled after the new
 *   on-demand update).
 *
 *   @author dliu
 */
class InstanceInfoReplicator implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(InstanceInfoReplicator.class);

    private final DiscoveryClient discoveryClient;
    private final InstanceInfo instanceInfo;

    private final int replicationIntervalSeconds;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<Future> scheduledPeriodicRef;

    private final AtomicBoolean started;
    private final RateLimiter rateLimiter;
    private final int burstSize;
    private final int allowedRatePerMinute;

    InstanceInfoReplicator(DiscoveryClient discoveryClient, InstanceInfo instanceInfo, int replicationIntervalSeconds, int burstSize) {
        this.discoveryClient = discoveryClient;
        this.instanceInfo = instanceInfo;
        this.scheduler = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("DiscoveryClient-InstanceInfoReplicator-%d")
                        .setDaemon(true)
                        .build());

        this.scheduledPeriodicRef = new AtomicReference<Future>();

        this.started = new AtomicBoolean(false);
        this.rateLimiter = new RateLimiter(TimeUnit.MINUTES);
        this.replicationIntervalSeconds = replicationIntervalSeconds;
        this.burstSize = burstSize;

        this.allowedRatePerMinute = 60 * this.burstSize / this.replicationIntervalSeconds;
        logger.info("InstanceInfoReplicator onDemand update allowed rate per min is {}", allowedRatePerMinute);
    }

    // 【启动复制器】DiscoveryClient 初始化定时任务时调用（initScheduledTasks）。
    // initialDelayMs 默认 40 秒 —— 客户端启动后并不会立刻注册，而是等实例信息收集完整后再注册
    public void start(int initialDelayMs) {
        // CAS 保证只启动一次
        if (started.compareAndSet(false, true)) {
            // 关键：先把实例信息标记为 dirty，这样首次调度执行 run() 时才会触发注册
            instanceInfo.setIsDirty();  // for initial register
            // 延迟 initialDelayMs 秒后执行 run()（即首次注册）
            Future next = scheduler.schedule(this, initialDelayMs, TimeUnit.SECONDS);
            scheduledPeriodicRef.set(next);
        }
    }

    public void stop() {
        shutdownAndAwaitTermination(scheduler);
        started.set(false);
    }

    private void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("InstanceInfoReplicator stop interrupted");
        }
    }

    // 【按需更新】实例状态发生变化时（如健康检查由 UP 变 DOWN）立即触发一次注册同步，
    // 不必等下一个 30s 周期。由 DiscoveryClient 中注册的 StatusChangeListener 调用。
    // 内置令牌桶限流（默认每分钟最多 4 次），防止状态抖动导致频繁注册
    public boolean onDemandUpdate() {
        // 限流检查：超过速率限制则忽略本次按需更新（等周期任务兜底）
        if (rateLimiter.acquire(burstSize, allowedRatePerMinute)) {
            if (!scheduler.isShutdown()) {
                scheduler.submit(new Runnable() {
                    @Override
                    public void run() {
                        logger.debug("Executing on-demand update of local InstanceInfo");

                        // 取消尚未执行的周期任务，避免按需更新与周期更新重复执行
                        // （run() 执行完会重新调度下一个周期任务）
                        Future latestPeriodic = scheduledPeriodicRef.get();
                        if (latestPeriodic != null && !latestPeriodic.isDone()) {
                            logger.debug("Canceling the latest scheduled update, it will be rescheduled at the end of on demand update");
                            latestPeriodic.cancel(false);
                        }

                        InstanceInfoReplicator.this.run();
                    }
                });
                return true;
            } else {
                logger.warn("Ignoring onDemand update due to stopped scheduler");
                return false;
            }
        } else {
            logger.warn("Ignoring onDemand update due to rate limiter");
            return false;
        }
    }

    // 【核心任务】"检查脏标记 → 注册"的循环，是客户端注册动作的真正发起点。
    // 工作模式：每次执行完都把自己重新调度到 replicationIntervalSeconds（默认 30s）之后，
    // 形成一个永不停止的自调度循环（而不是用 scheduleAtFixedRate，好处是任务异常/耗时不会堆积）
    public void run() {
        try {
            // 刷新本地实例信息：重新读取主机名/IP/租约配置，检查健康检查状态，
            // 若有变化会把 instanceInfo 标记为 dirty
            discoveryClient.refreshInstanceInfo();

            // 实例信息是脏的（首次启动 / 信息变更 / 状态变更）→ 需要向服务端同步
            Long dirtyTimestamp = instanceInfo.isDirtyWithTime();
            if (dirtyTimestamp != null) {
                // 发起注册（注册即同步：Eureka 用"重新注册"来更新服务端的实例信息）
                discoveryClient.register();
                // 清除脏标记（带时间戳的 CAS 操作：若期间又被标脏，则不清除，下个周期继续注册）
                instanceInfo.unsetIsDirty(dirtyTimestamp);
            }
        } catch (Throwable t) {
            logger.warn("There was a problem with the instance info replicator", t);
        } finally {
            // 无论成功失败，都调度下一次执行（自调度循环）
            Future next = scheduler.schedule(this, replicationIntervalSeconds, TimeUnit.SECONDS);
            scheduledPeriodicRef.set(next);
        }
    }

}
