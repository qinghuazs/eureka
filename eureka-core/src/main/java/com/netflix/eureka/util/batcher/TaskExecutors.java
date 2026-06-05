package com.netflix.eureka.util.batcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.eureka.util.batcher.TaskProcessor.ProcessingResult;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.stats.StatsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.eureka.Names.METRIC_REPLICATION_PREFIX;

/**
 * {@link TaskExecutors} instance holds a number of worker threads that cooperate with {@link AcceptorExecutor}.
 * Each worker sends a job request to {@link AcceptorExecutor} whenever it is available, and processes it once
 * provided with a task(s).
 *
 * @author Tomasz Bak
 */
// 【批处理框架的"执行端"】与 AcceptorExecutor（攒批端）配套，构成生产者-消费者模型的消费侧。
//
//   职责分工（文档05/09 论断）：
//     AcceptorExecutor 负责"攒批 + 去重 + 限流"——把海量复制请求合并、丢弃重复、按节奏放行；
//     TaskExecutors   负责"把攒好的批真正执行掉、失败的回收重试"——发起实际的 HTTP 复制请求。
//   两者合起来把发往 peer 节点的复制请求数降低 95%+（文档05 的 250:1 合并比：
//     一批最多 250 个任务合并为 1 次请求）。
//
//   本类只是一个"工作线程池容器"：构造时按 workerCount 拉起若干 daemon 线程，
//   每个线程跑一个 WorkerRunnable，循环地"向 AcceptorExecutor 主动拉活儿 → 执行 → 回报结果"。
//   注意是 pull 模型（worker 主动要任务），这样保证拿到的永远是去重合并后的最新数据，不会处理陈旧任务。
//
//   两种执行路径：
//     BatchWorkerRunnable      —— 批处理模式，一次取一【批】任务合并成一次请求（集群复制走这条）；
//     SingleTaskWorkerRunnable —— 单任务模式，一次只取一个任务执行（非批处理场景）。
class TaskExecutors<ID, T> {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutors.class);

    private static final Map<String, TaskExecutorMetrics> registeredMonitors = new HashMap<>();

    private final AtomicBoolean isShutdown;
    private final List<Thread> workerThreads;

    // 构造即启动：按 workerCount 拉起一组工作线程，每条线程进入各自 WorkerRunnable 的 run() 死循环开始干活。
    // 工厂模式（workerRunnableFactory）解耦了"线程池怎么管"与"每个 worker 具体跑批处理还是单任务"。
    TaskExecutors(WorkerRunnableFactory<ID, T> workerRunnableFactory, int workerCount, AtomicBoolean isShutdown) {
        this.isShutdown = isShutdown;
        this.workerThreads = new ArrayList<>();

        ThreadGroup threadGroup = new ThreadGroup("eurekaTaskExecutors");
        for (int i = 0; i < workerCount; i++) {
            // 全部设为 daemon 守护线程：进程退出时不会被这些后台复制线程拖住。
            WorkerRunnable<ID, T> runnable = workerRunnableFactory.create(i);
            Thread workerThread = new Thread(threadGroup, runnable, runnable.getWorkerName());
            workerThreads.add(workerThread);
            workerThread.setDaemon(true);
            workerThread.start();
        }
    }

    void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            for (Thread workerThread : workerThreads) {
                workerThread.interrupt();
            }
            registeredMonitors.forEach(Monitors::unregisterObject);
        }
    }

    // 工厂方法（单任务版）：构造一组跑 SingleTaskWorkerRunnable 的执行线程，每次只处理一个任务。
    static <ID, T> TaskExecutors<ID, T> singleItemExecutors(final String name,
                                                            int workerCount,
                                                            final TaskProcessor<T> processor,
                                                            final AcceptorExecutor<ID, T> acceptorExecutor) {
        final AtomicBoolean isShutdown = new AtomicBoolean();
        final TaskExecutorMetrics metrics = new TaskExecutorMetrics(name);
        registeredMonitors.put(name, metrics);
        return new TaskExecutors<>(idx -> new SingleTaskWorkerRunnable<>("TaskNonBatchingWorker-" + name + '-' + idx, isShutdown, metrics, processor, acceptorExecutor), workerCount, isShutdown);
    }

    // 工厂方法（批处理版）：集群复制走的就是这条——构造一组跑 BatchWorkerRunnable 的执行线程，
    // 每次取一整批任务合并成一次请求，这是把复制请求数降低 95%+ 的关键所在。
    static <ID, T> TaskExecutors<ID, T> batchExecutors(final String name,
                                                       int workerCount,
                                                       final TaskProcessor<T> processor,
                                                       final AcceptorExecutor<ID, T> acceptorExecutor) {
        final AtomicBoolean isShutdown = new AtomicBoolean();
        final TaskExecutorMetrics metrics = new TaskExecutorMetrics(name);
        registeredMonitors.put(name, metrics);
        return new TaskExecutors<>(idx -> new BatchWorkerRunnable<>("TaskBatchingWorker-" + name + '-' + idx, isShutdown, metrics, processor, acceptorExecutor), workerCount, isShutdown);
    }

    // 执行端的 Servo 监控指标：把每批/每个任务的执行结果按 成功/瞬时错误/永久错误/拥塞 四类分别计数，
    // 并记录任务"从提交到被执行"的等待耗时分位数（p50/p95/p99/p99.5），用于观测复制链路的健康度与积压。
    static class TaskExecutorMetrics {

        @Monitor(name = METRIC_REPLICATION_PREFIX + "numberOfSuccessfulExecutions", description = "Number of successful task executions", type = DataSourceType.COUNTER)
        volatile long numberOfSuccessfulExecutions;

        @Monitor(name = METRIC_REPLICATION_PREFIX + "numberOfTransientErrors", description = "Number of transient task execution errors", type = DataSourceType.COUNTER)
        volatile long numberOfTransientError;

        @Monitor(name = METRIC_REPLICATION_PREFIX + "numberOfPermanentErrors", description = "Number of permanent task execution errors", type = DataSourceType.COUNTER)
        volatile long numberOfPermanentError;

        @Monitor(name = METRIC_REPLICATION_PREFIX + "numberOfCongestionIssues", description = "Number of congestion issues during task execution", type = DataSourceType.COUNTER)
        volatile long numberOfCongestionIssues;

        final StatsTimer taskWaitingTimeForProcessing;

        TaskExecutorMetrics(String id) {
            final double[] percentiles = {50.0, 95.0, 99.0, 99.5};
            final StatsConfig statsConfig = new StatsConfig.Builder()
                    .withSampleSize(1000)
                    .withPercentiles(percentiles)
                    .withPublishStdDev(true)
                    .build();
            final MonitorConfig config = MonitorConfig.builder(METRIC_REPLICATION_PREFIX + "executionTime").build();
            taskWaitingTimeForProcessing = new StatsTimer(config, statsConfig);

            try {
                Monitors.registerObject(id, this);
            } catch (Throwable e) {
                logger.warn("Cannot register servo monitor for this object", e);
            }
        }

        void registerTaskResult(ProcessingResult result, int count) {
            switch (result) {
                case Success:
                    numberOfSuccessfulExecutions += count;
                    break;
                case TransientError:
                    numberOfTransientError += count;
                    break;
                case PermanentError:
                    numberOfPermanentError += count;
                    break;
                case Congestion:
                    numberOfCongestionIssues += count;
                    break;
            }
        }

        <ID, T> void registerExpiryTime(TaskHolder<ID, T> holder) {
            taskWaitingTimeForProcessing.record(System.currentTimeMillis() - holder.getSubmitTimestamp(), TimeUnit.MILLISECONDS);
        }

        <ID, T> void registerExpiryTimes(List<TaskHolder<ID, T>> holders) {
            long now = System.currentTimeMillis();
            for (TaskHolder<ID, T> holder : holders) {
                taskWaitingTimeForProcessing.record(now - holder.getSubmitTimestamp(), TimeUnit.MILLISECONDS);
            }
        }
    }

    interface WorkerRunnableFactory<ID, T> {
        WorkerRunnable<ID, T> create(int idx);
    }

    abstract static class WorkerRunnable<ID, T> implements Runnable {
        final String workerName;
        final AtomicBoolean isShutdown;
        final TaskExecutorMetrics metrics;
        final TaskProcessor<T> processor;
        final AcceptorExecutor<ID, T> taskDispatcher;

        WorkerRunnable(String workerName,
                       AtomicBoolean isShutdown,
                       TaskExecutorMetrics metrics,
                       TaskProcessor<T> processor,
                       AcceptorExecutor<ID, T> taskDispatcher) {
            this.workerName = workerName;
            this.isShutdown = isShutdown;
            this.metrics = metrics;
            this.processor = processor;
            this.taskDispatcher = taskDispatcher;
        }

        String getWorkerName() {
            return workerName;
        }
    }

    // 【批处理执行单元】集群复制的实际执行者：循环地取一【批】任务、合并成一次请求发出去、按结果决定重试或丢弃。
    // 这是 250:1 合并比落地的地方——AcceptorExecutor 已经把批攒好，这里把整批交给 processor 一次性执行掉。
    static class BatchWorkerRunnable<ID, T> extends WorkerRunnable<ID, T> {

        BatchWorkerRunnable(String workerName,
                            AtomicBoolean isShutdown,
                            TaskExecutorMetrics metrics,
                            TaskProcessor<T> processor,
                            AcceptorExecutor<ID, T> acceptorExecutor) {
            super(workerName, isShutdown, metrics, processor, acceptorExecutor);
        }

        // 批处理工作线程主循环：从 AcceptorExecutor 拉取一【批】任务 → 合并成一次请求执行 → 按结果决定回流重试或丢弃。
        @Override
        public void run() {
            try {
                while (!isShutdown.get()) {
                    // 1) 主动向调度端拉一整批任务（pull 模型，拿到的是去重合并后的最新数据），并记录这批任务的排队等待耗时。
                    List<TaskHolder<ID, T>> holders = getWork();
                    metrics.registerExpiryTimes(holders);

                    // 2) 把这批 TaskHolder 解包成纯任务列表，整批交给 processor 一次性执行——即合并为一次 HTTP 复制请求发往 peer。
                    List<T> tasks = getTasksOf(holders);
                    ProcessingResult result = processor.process(tasks);
                    // 3) 按执行结果决定后续动作（结果语义见 TaskProcessor.ProcessingResult）：
                    switch (result) {
                        case Success:
                            // 成功：整批完成，无需任何处理。
                            break;
                        case Congestion:
                        case TransientError:
                            // 拥塞 / 瞬时错误：属于"可恢复"失败，把整批原样回流到 AcceptorExecutor 的重做队列。
                            // 回流后这批任务会与新任务再次去重合并（若某实例已有更新版本，旧任务被丢弃），稍后由 TrafficShaper 控制节奏重发。
                            taskDispatcher.reprocess(holders, result);
                            break;
                        case PermanentError:
                            // 永久错误：不可恢复（如对端返回 4xx），重试也没用，直接丢弃整批，仅打日志。
                            logger.warn("Discarding {} tasks of {} due to permanent error", holders.size(), workerName);
                    }
                    // 4) 把本批结果按类别累加进监控指标。
                    metrics.registerTaskResult(result, tasks.size());
                }
            } catch (InterruptedException e) {
                // shutdown 时被 interrupt 唤醒，正常退出循环，无需处理。
                // Ignore
            } catch (Throwable e) {
                // 兜底：任何意外异常都不让工作线程"裸退"，否则该 worker 静默死掉、复制会悄无声息地积压。
                // Safe-guard, so we never exit this loop in an uncontrolled way.
                logger.warn("Discovery WorkerThread error", e);
            }
        }

        // 向 AcceptorExecutor 发起"我要干活"的请求并阻塞等待一批任务。
        // requestWorkItems() 这次调用本身就是给调度端的"领活信号"，调度端据此把攒好的一批塞进 workQueue。
        // 用 poll(1s) 而非无限阻塞：以便每秒回头检查 isShutdown，能及时响应停机；空轮询则继续等下一批。
        private List<TaskHolder<ID, T>> getWork() throws InterruptedException {
            BlockingQueue<List<TaskHolder<ID, T>>> workQueue = taskDispatcher.requestWorkItems();
            List<TaskHolder<ID, T>> result;
            do {
                result = workQueue.poll(1, TimeUnit.SECONDS);
            } while (!isShutdown.get() && result == null);
            return (result == null) ? new ArrayList<>() : result;
        }

        // 解包：从 TaskHolder（携带 id/提交时间戳等元数据的包装）中取出真正要执行的任务对象，组成给 processor 的批列表。
        private List<T> getTasksOf(List<TaskHolder<ID, T>> holders) {
            List<T> tasks = new ArrayList<>(holders.size());
            for (TaskHolder<ID, T> holder : holders) {
                tasks.add(holder.getTask());
            }
            return tasks;
        }
    }

    // 【单任务执行单元】批处理的"退化版"：一次只拉一个任务、执行一个。
    // 结构与 BatchWorkerRunnable 完全对称，区别仅在于"批列表"换成了"单个 TaskHolder"，失败回流、丢弃语义一致。
    static class SingleTaskWorkerRunnable<ID, T> extends WorkerRunnable<ID, T> {

        SingleTaskWorkerRunnable(String workerName,
                                 AtomicBoolean isShutdown,
                                 TaskExecutorMetrics metrics,
                                 TaskProcessor<T> processor,
                                 AcceptorExecutor<ID, T> acceptorExecutor) {
            super(workerName, isShutdown, metrics, processor, acceptorExecutor);
        }

        // 单任务工作线程主循环：从 AcceptorExecutor 拉取一个任务 → 执行 → 按结果决定回流重试或丢弃。
        @Override
        public void run() {
            try {
                while (!isShutdown.get()) {
                    // 1) 主动领一个任务（pull 模型）。poll(1s) 空转期间持续检查 isShutdown，停机时直接 return 退出线程。
                    BlockingQueue<TaskHolder<ID, T>> workQueue = taskDispatcher.requestWorkItem();
                    TaskHolder<ID, T> taskHolder;
                    while ((taskHolder = workQueue.poll(1, TimeUnit.SECONDS)) == null) {
                        if (isShutdown.get()) {
                            return;
                        }
                    }
                    // 2) 记录该任务的排队等待耗时。
                    metrics.registerExpiryTime(taskHolder);
                    if (taskHolder != null) {
                        // 3) 取出任务执行；后续按结果分支与批处理版完全一致。
                        ProcessingResult result = processor.process(taskHolder.getTask());
                        switch (result) {
                            case Success:
                                // 成功：完成。
                                break;
                            case Congestion:
                            case TransientError:
                                // 可恢复失败：把这个任务回流到重做队列，与新任务合并去重后稍后重试。
                                taskDispatcher.reprocess(taskHolder, result);
                                break;
                            case PermanentError:
                                // 不可恢复失败：直接丢弃，仅打日志。
                                logger.warn("Discarding a task of {} due to permanent error", workerName);
                        }
                        // 4) 累加监控（单任务故 count=1）。
                        metrics.registerTaskResult(result, 1);
                    }
                }
            } catch (InterruptedException e) {
                // shutdown 时被 interrupt 唤醒，正常退出。
                // Ignore
            } catch (Throwable e) {
                // 兜底：绝不让工作线程因意外异常裸退而静默死掉。
                // Safe-guard, so we never exit this loop in an uncontrolled way.
                logger.warn("Discovery WorkerThread error", e);
            }
        }
    }
}
