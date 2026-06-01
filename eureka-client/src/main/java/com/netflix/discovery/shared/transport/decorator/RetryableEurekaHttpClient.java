/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.transport.decorator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.discovery.shared.resolver.ClusterResolver;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpClientFactory;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import com.netflix.discovery.shared.transport.EurekaTransportConfig;
import com.netflix.discovery.shared.transport.TransportClientFactory;
import com.netflix.discovery.shared.transport.TransportException;
import com.netflix.discovery.shared.transport.TransportUtils;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.discovery.EurekaClientNames.METRIC_TRANSPORT_PREFIX;

/**
 * {@link RetryableEurekaHttpClient} retries failed requests on subsequent servers in the cluster.
 * It maintains also simple quarantine list, so operations are not retried again on servers
 * that are not reachable at the moment.
 * <h3>Quarantine</h3>
 * All the servers to which communication failed are put on the quarantine list. First successful execution
 * clears this list, which makes those server eligible for serving future requests.
 * The list is also cleared once all available servers are exhausted.
 * <h3>5xx</h3>
 * If 5xx status code is returned, {@link ServerStatusEvaluator} predicate evaluates if the retries should be
 * retried on another server, or the response with this status code returned to the client.
 *
 * @author Tomasz Bak
 * @author Li gang
 */
public class RetryableEurekaHttpClient extends EurekaHttpClientDecorator {

    private static final Logger logger = LoggerFactory.getLogger(RetryableEurekaHttpClient.class);

    public static final int DEFAULT_NUMBER_OF_RETRIES = 3;

    private final String name;
    private final EurekaTransportConfig transportConfig;
    private final ClusterResolver clusterResolver;
    private final TransportClientFactory clientFactory;
    private final ServerStatusEvaluator serverStatusEvaluator;
    private final int numberOfRetries;

    private final AtomicReference<EurekaHttpClient> delegate = new AtomicReference<>();

    private final Set<EurekaEndpoint> quarantineSet = new ConcurrentSkipListSet<>();

    public RetryableEurekaHttpClient(String name,
                                     EurekaTransportConfig transportConfig,
                                     ClusterResolver clusterResolver,
                                     TransportClientFactory clientFactory,
                                     ServerStatusEvaluator serverStatusEvaluator,
                                     int numberOfRetries) {
        this.name = name;
        this.transportConfig = transportConfig;
        this.clusterResolver = clusterResolver;
        this.clientFactory = clientFactory;
        this.serverStatusEvaluator = serverStatusEvaluator;
        this.numberOfRetries = numberOfRetries;
        Monitors.registerObject(name, this);
    }

    @Override
    public void shutdown() {
        TransportUtils.shutdown(delegate.get());
        if(Monitors.isObjectRegistered(name, this)) {
            Monitors.unregisterObject(name, this);
        }
    }

    // 【重试层装饰器】客户端高可用的核心：单台 Server 故障时自动切换到下一台。
    // 工作机制：
    //   1. 正常情况：复用上次成功的客户端（delegate），不做任何额外操作
    //   2. 失败时（连接异常或 5xx）：把当前 Server 加入"隔离区"，换下一个候选 Server 重试
    //   3. 最多重试 3 次（DEFAULT_NUMBER_OF_RETRIES），耗尽后抛 TransportException
    // 这就是"配置了多个 eureka serviceUrl 后，挂一台不影响客户端"的实现原理
    @Override
    protected <R> EurekaHttpResponse<R> execute(RequestExecutor<R> requestExecutor) {
        List<EurekaEndpoint> candidateHosts = null;
        int endpointIdx = 0;
        for (int retry = 0; retry < numberOfRetries; retry++) {
            // delegate 缓存了上次成功的客户端：正常情况下直接复用，不重新选 Server
            EurekaHttpClient currentHttpClient = delegate.get();
            EurekaEndpoint currentEndpoint = null;
            if (currentHttpClient == null) {
                // 没有可复用的客户端（首次请求 或 上次失败被清空）→ 从候选列表选一个 Server
                if (candidateHosts == null) {
                    candidateHosts = getHostCandidates();
                    if (candidateHosts.isEmpty()) {
                        throw new TransportException("There is no known eureka server; cluster server list is empty");
                    }
                }
                if (endpointIdx >= candidateHosts.size()) {
                    // 所有候选 Server 都试过了仍失败
                    throw new TransportException("Cannot execute request on any known server");
                }

                // 按顺序取下一个候选 Server，为它创建客户端（内层是重定向层→Jersey）
                currentEndpoint = candidateHosts.get(endpointIdx++);
                currentHttpClient = clientFactory.newClient(currentEndpoint);
            }

            try {
                EurekaHttpResponse<R> response = requestExecutor.execute(currentHttpClient);
                // 状态评估器判断响应是否可接受（非 5xx 等）
                if (serverStatusEvaluator.accept(response.getStatusCode(), requestExecutor.getRequestType())) {
                    // 成功：缓存这个客户端供后续请求复用
                    delegate.set(currentHttpClient);
                    if (retry > 0) {
                        logger.info("Request execution succeeded on retry #{}", retry);
                    }
                    return response;
                }
                logger.warn("Request execution failure with status code {}; retrying on another server if available", response.getStatusCode());
            } catch (Exception e) {
                logger.warn("Request execution failed with message: {}", e.getMessage());  // just log message as the underlying client should log the stacktrace
            }

            // Connection error or 5xx from the server that must be retried on another server
            // 失败处理：清空缓存的客户端 + 把这台 Server 加入隔离区（下次选候选时排除它）
            delegate.compareAndSet(currentHttpClient, null);
            if (currentEndpoint != null) {
                quarantineSet.add(currentEndpoint);
            }
        }
        throw new TransportException("Retry limit reached; giving up on completing the request");
    }

    public static EurekaHttpClientFactory createFactory(final String name,
                                                        final EurekaTransportConfig transportConfig,
                                                        final ClusterResolver<EurekaEndpoint> clusterResolver,
                                                        final TransportClientFactory delegateFactory,
                                                        final ServerStatusEvaluator serverStatusEvaluator) {
        return new EurekaHttpClientFactory() {
            @Override
            public EurekaHttpClient newClient() {
                return new RetryableEurekaHttpClient(name, transportConfig, clusterResolver, delegateFactory,
                        serverStatusEvaluator, DEFAULT_NUMBER_OF_RETRIES);
            }

            @Override
            public void shutdown() {
                delegateFactory.shutdown();
            }
        };
    }

    // 【候选 Server 选择 + 隔离区管理】
    // 隔离区(quarantineSet)：最近失败过的 Server 集合，选候选时排除它们。
    // 自愈机制：当隔离的 Server 数量达到阈值（默认 66% 的 Server 都被隔离了），
    // 说明可能不是 Server 的问题（比如客户端自己网络抖动），清空隔离区重新尝试所有 Server
    private List<EurekaEndpoint> getHostCandidates() {
        // 从解析器获取全部 Server 列表（来源：配置的 serviceUrl / DNS）
        List<EurekaEndpoint> candidateHosts = clusterResolver.getClusterEndpoints();
        // 清理隔离区中已经不在集群列表里的 Server（集群缩容场景）
        quarantineSet.retainAll(candidateHosts);

        // If enough hosts are bad, we have no choice but start over again
        // 隔离阈值 = Server 总数 × 66%（retryableClientQuarantineRefreshPercentage 默认 0.66）
        int threshold = (int) (candidateHosts.size() * transportConfig.getRetryableClientQuarantineRefreshPercentage());
        //Prevent threshold is too large
        if (threshold > candidateHosts.size()) {
            threshold = candidateHosts.size();
        }
        if (quarantineSet.isEmpty()) {
            // no-op
        } else if (quarantineSet.size() >= threshold) {
            // 隔离的太多了 → 清空隔离区，给所有 Server 重新机会
            logger.debug("Clearing quarantined list of size {}", quarantineSet.size());
            quarantineSet.clear();
        } else {
            // 正常情况：候选列表 = 全部 Server - 隔离区
            List<EurekaEndpoint> remainingHosts = new ArrayList<>(candidateHosts.size());
            for (EurekaEndpoint endpoint : candidateHosts) {
                if (!quarantineSet.contains(endpoint)) {
                    remainingHosts.add(endpoint);
                }
            }
            candidateHosts = remainingHosts;
        }

        return candidateHosts;
    }

    @Monitor(name = METRIC_TRANSPORT_PREFIX + "quarantineSize",
            description = "number of servers quarantined", type = DataSourceType.GAUGE)
    public long getQuarantineSetSize() {
        return quarantineSet.size();
    }
}
