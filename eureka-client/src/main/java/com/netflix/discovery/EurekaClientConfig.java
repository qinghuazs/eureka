/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.discovery;

import java.util.List;

import javax.annotation.Nullable;

import com.google.inject.ImplementedBy;
import com.netflix.discovery.shared.transport.EurekaTransportConfig;

/**
 * Configuration information required by the eureka clients to register an
 * instance with <em>Eureka</em> server.
 *
 * <p>
 * Most of the required information is provided by the default configuration
 * {@link DefaultEurekaClientConfig}. The users just need to provide the eureka
 * server service urls. The Eureka server service urls can be configured by 2
 * mechanisms
 *
 * 1) By registering the information in the DNS. 2) By specifying it in the
 * configuration.
 * </p>
 *
 *
 * Once the client is registered, users can look up information from
 * {@link EurekaClient} based on <em>virtual hostname</em> (also called
 * VIPAddress), the most common way of doing it or by other means to get the
 * information necessary to talk to other instances registered with
 * <em>Eureka</em>.
 *
 * <p>
 * Note that all configurations are not effective at runtime unless and
 * otherwise specified.
 * </p>
 *
 * @author Karthik Ranganathan
 *
 */
@ImplementedBy(DefaultEurekaClientConfig.class)
public interface EurekaClientConfig {

    /**
     * Indicates how often(in seconds) to fetch the registry information from
     * the eureka server.
     *
     * @return the fetch interval in seconds.
     */
    // 【注册表拉取周期 · 默认 30s】DiscoveryClient 启动后，cacheRefreshTask 定时任务按本值周期
    // 拉取注册表（来自 Eureka 服务端）。每次拉取都会走 fetchRegistry()，决策"全量 vs 增量"。
    // 调大此值 → 本地缓存更新周期变长，服务发现延迟增加，但服务端和客户端网络压力减轻；
    // 调小此值 → 感知新上线/下线/异常实例的速度更快，代价是网络和 CPU 消耗增加、缓存频繁变化。
    // 配置场景：特别流量突增需紧密跟踪实例变化 → 调小；大规模集群/网络不稳定 → 调大。
    // 调用链：DiscoveryClient.initScheduledTasks → new TimedSupervisorTask(..., registryFetchIntervalSeconds, ...)。
    // 配置项 client.refresh.interval。注意与 getInstanceInfoReplicationIntervalSeconds()(本实例状态同步周期，默认 30s)
    //   和 getEurekaServiceUrlPollIntervalSeconds()(服务端 URL 刷新周期，默认 5 分钟) 的区别。
    int getRegistryFetchIntervalSeconds();

    /**
     * Indicates how often(in seconds) to replicate instance changes to be
     * replicated to the eureka server.
     *
     * @return the instance replication interval in seconds.
     */
    // 【周期复制延迟 · 默认 30s】InstanceInfoReplicator 将本实例的 InstanceInfo（实例元数据/状态）周期性
    // 同步(注册)到服务端的间隔秒数；对应的首次启动延迟由 getInitialInstanceInfoReplicationIntervalSeconds()
    // 控制(默认 40s)——两者成对配置，首次更长给信息就绪留缓冲，然后改用该值作为周期。
    // 调大该值：减少与服务端的通信频率，适用于实例信息很少变化的场景；调小(如 10s)：状态变更同步更快。
    // 坑：这是"周期间隔"而非"最大延迟"，即使网络抖动也不会因超时退避而推迟，只有心跳/注册表拉取那两个循环用
    // TimedSupervisorTask 包装才有指数退避——InstanceInfoReplicator 本身用普通 scheduler.schedule() 实现。
    // 配置项 appinfo.replicate.interval。调用链：DiscoveryClient.initScheduledTasks →
    //   InstanceInfoReplicator(本值) → InstanceInfoReplicator.run() 中的 scheduler.schedule(本值)。
    int getInstanceInfoReplicationIntervalSeconds();

    /**
     * Indicates how long initially (in seconds) to replicate instance info
     * to the eureka server
     */
    // 【首次复制延迟 · 默认 40s】InstanceInfoReplicator 启动后、第一次把本实例 InstanceInfo 同步(注册)到
    // 服务端之前要等待的秒数；其后的周期间隔由上面的 getInstanceInfoReplicationIntervalSeconds() 控制(默认 30s)。
    // 为什么首次要单独留延迟、且比周期长(40>30)？给客户端启动后留一个"信息就绪"窗口——等实例信息收集完整、
    // 状态/健康检查就绪后再用"成品"信息注册，避免一启动就把半成品上报上去。
    // 调用链：DiscoveryClient.initScheduledTasks → instanceInfoReplicator.start(本值)：start() 先把 instanceInfo
    //   标脏，延迟本值(秒)后执行 run() 触发首次 register()（Eureka 用"重新注册"来同步实例信息）。
    // 配置项 appinfo.initial.replicate.time。坑：start 的形参名叫 initialDelayMs 带 Ms 后缀，实际却按
    //   TimeUnit.SECONDS 调度——单位是"秒"不是毫秒，别被名字误导。
    int getInitialInstanceInfoReplicationIntervalSeconds();

    /**
     * Indicates how often(in seconds) to poll for changes to eureka server
     * information.
     *
     * <p>
     * Eureka servers could be added or removed and this setting controls how
     * soon the eureka clients should know about it.
     * </p>
     *
     * @return the interval to poll for eureka service url changes.
     */
    // 【服务端列表轮询周期 · 默认 300s】Eureka 服务集群成员动态变更时，客户端多久重新轮询一次服务列表
    // (以秒为单位)。当服务端被加入/移除时，客户端通过此周期检测这些变化并更新本地的 Server 列表。
    // 调大此值 → Server 列表变更的感知时间延长，优点是减少网络负载；调小此值 → 感知延迟降低，
    // 但轮询频率升高。典型场景：灰度上线/下线服务端时，如果需要快速应对可调小到 30-60s；如果是
    // 稳定集群调大到 5-10 分钟。
    // 调用链：LegacyClusterResolver.<init> 和 EurekaHttpClients.queryClientFactory 都会
    //   用此值作为 ReloadingClusterResolver 的「重载间隔」，定期尝试重新解析 Server 列表。
    // 配置项 serviceUrlPollIntervalMs (注意单位是「毫秒」，代码里除以1000后返回秒)。
    int getEurekaServiceUrlPollIntervalSeconds();

    /**
     * Gets the proxy host to eureka server if any.
     *
     * @return the proxy host.
     */
    String getProxyHost();

    /**
     * Gets the proxy port to eureka server if any.
     *
     * @return the proxy port.
     */
    String getProxyPort();

    /**
     * Gets the proxy user name if any.
     *
     * @return the proxy user name.
     */
    String getProxyUserName();

    /**
     * Gets the proxy password if any.
     *
     * @return the proxy password.
     */
    String getProxyPassword();

    /**
     * Indicates whether the content fetched from eureka server has to be
     * compressed whenever it is supported by the server. The registry
     * information from the eureka server is compressed for optimum network
     * traffic.
     *
     * @return true, if the content need to be compressed, false otherwise.
     * @deprecated gzip content encoding will be always enforced in the next minor Eureka release (see com.netflix.eureka.GzipEncodingEnforcingFilter).
     */
    // 【响应压缩开关 · 默认 true(启用)】控制是否对 Eureka 服务端的响应内容启用 GZip 压缩
    // ；默认 true 时，客户端会发送『Accept-Encoding: gzip』请求头，服务端返回 gzip 压缩的
    // 注册表(registry)。注册表 JSON 往往数 MB，gzip 可压缩到 1/5-1/10，节省网络带宽尤其是
    // 移动网络。设为 false 时，客户端接收未压缩的原始数据，增加网络传输、但减轻 CPU 解压成本
    // (现代 CPU 对 gzip 优化很好，通常收益大于成本)。【已废弃】——JavaDoc 标记此方法
    // @deprecated，Eureka 未来版本会『强制启用 GZip』(见 GzipEncodingEnforcingFilter)
    // ，所以此开关最终会失效。
    // 调用链：JerseyEurekaHttpClientFactory.JerseyEurekaHttpClientFactoryBuilder.addFilters()
    //   中自动向 ApacheHttpClient4 添加『GZIPContentEncodingFilter(false)』——注意参数
    //   false 的含义是『在解码时自动解压』，与 shouldGZipContent() 的值无关(当前未读取本配置)。
    // 配置项 eurekaServer.gzipContent。
    // 坑：当前客户端实现『忽视了』本配置项的值——始终启用 GZip(通过 GZIPContentEncodingFilter)、
    //   不受 shouldGZipContent() 控制；该配置存在但未生效，可视为『遗留代码』。
    boolean shouldGZipContent();

    /**
     * Indicates how long to wait (in seconds) before a read from eureka server
     * needs to timeout.
     *
     * @return time in seconds before the read should timeout.
     */
    // 【读超时 · 默认 8s】从 Eureka 服务端读取响应数据的最大等待时间(秒)；若服务端在 8s 内未返回
    // 数据，客户端连接会被中止。调小此值(如改 3s)会导致网络波动大的环境中更易超时失败；调大(如改
    // 15s)可提高容错但增加平均延迟。常见调整场景：注册表特别大(几千实例)、网络延迟高、或目标
    // 服务端处理缓慢时，建议调整为 15-20s。与下面 getEurekaServerConnectTimeoutSeconds() 都是
    // Apache HttpClient 连接级别的超时，前者管读数据、后者管建立连接的时间。
    // 调用链：EurekaClientFactoryBuilder.withClientConfig() → withReadTimeout(本值*1000)
    //   → JerseyEurekaHttpClientFactory 设置给 ApacheHttpClient4 client；在 DiscoveryClient
    //   的 makeRemoteCall() 系列方法中被触发。
    // 配置项 eurekaServer.readTimeout(单位秒)。
    // 坑：DefaultEurekaClientConfig 实际读的是整数，乘以 1000 后才传给 HttpClient（毫秒单位）。
    int getEurekaServerReadTimeoutSeconds();

    /**
     * Indicates how long to wait (in seconds) before a connection to eureka
     * server needs to timeout.
     *
     * <p>
     * Note that the connections in the client are pooled by
     * {@link org.apache.http.client.HttpClient} and this setting affects the actual
     * connection creation and also the wait time to get the connection from the
     * pool.
     * </p>
     *
     * @return time in seconds before the connections should timeout.
     */
    // 【连接超时 · 默认 5s】与 Eureka 服务端建立 TCP 连接的最大等待时间(秒)；也会影响从连接
    // 池获取可用连接的等待时间(see JavaDoc)。调小到 2-3s 会在网络不稳定/服务端响应慢时频繁超时；
    // 调大到 10s+ 可避免超时但会拖累整个请求延迟。一般不需调整，除非目标 Eureka 集群处于高
    // 负载或地理位置遥远。与 getEurekaServerReadTimeoutSeconds() 的区别：前者管连接建立、
    // 后者管读数据；两者都超时了连接才会最终失败。
    // 调用链：同上 EurekaClientFactoryBuilder.withClientConfig() → withConnectionTimeout(本值*1000)
    //   → JerseyEurekaHttpClientFactory/EurekaJerseyClientImpl 配置 ApacheHttpClient4。
    // 配置项 eurekaServer.connectTimeout(单位秒)。
    // 坑：EurekaJerseyClientImpl 文档提到会影响『从连接池等待一个连接的时间』——这是 HttpClient
    //   连接池的特性；若连接都被占用，新请求会等待 connectTimeout，超时后抛异常。
    int getEurekaServerConnectTimeoutSeconds();

    /**
     * Gets the name of the implementation which implements
     * {@link BackupRegistry} to fetch the registry information as a fall back
     * option for only the first time when the eureka client starts.
     *
     * <p>
     * This may be needed for applications which needs additional resiliency for
     * registry information without which it cannot operate.
     * </p>
     *
     * @return the class name which implements {@link BackupRegistry}.
     */
    // 【备用注册表实现类名 · 默认无(null)】当客户端首次启动时从服务端获取注册表失败时，
    // 会尝试从此配置指定的「备用注册表」(BackupRegistry 接口实现)获取列表。此实现类应在 classpath
    // 上可找到，且必须有无参构造器。若此值为 null 或指定的类加载失败，客户端会使用默认的
    // NotImplementedRegistryImpl(什么都不做)作为兜底。
    // 用途：针对需要极高可用性、不能容忍无注册表启动的应用，可实现一个 BackupRegistry 从本地文件/缓存/
    // 配置服务等读出上次保存的注册表快照，保证即使连不上 Eureka 服务也能部分可用。
    // 调大/调小/开关影响：配置合法的实现类名 → DiscoveryClient 启动时会尝试反射加载并实例化；
    // 失败时记录错误日志但不抛异常(服务可继续用 fallback 兜底)；配置为 null → 跳过 backup 逻辑。
    // 场景：金融/支付等对可用性极端敏感的服务；网络极不稳定的边缘场景(如离线模式)。
    // 调用链：DiscoveryClient.<init> 中 Provider<BackupRegistry> 的 get() 方法:
    //   String backupRegistryClassName = config.getBackupRegistryImpl();
    //   if (null != backupRegistryClassName) { ... backupRegistryInstance = (BackupRegistry) Class.forName(backupRegistryClassName).newInstance(); }
    //   fetchRegistry() 失败时会调 backupRegistryProvider.get().fetch() 尝试兜底。
    // 配置项 backupregistry。
    // 坑：需确保指定的类在 classpath 上且能正常构造，否则启动日志会有大量 ERROR。
    String getBackupRegistryImpl();

    /**
     * Gets the total number of connections that is allowed from eureka client
     * to all eureka servers.
     *
     * @return total number of allowed connections from eureka client to all
     *         eureka servers.
     */
    // 【连接池总容量 · 默认 200】Apache HttpClient 连接管理器维护的『全局』连接池大小上限
    // ——所有 Eureka 服务端(含 primary+backup+redirect)的连接数之和不超过此值。若 Eureka 集群
    // 有 N 个节点、每个心跳/注册请求都需要新连接，200 的容量足够处理并发请求。但若应用高并发、
    // 同时发起 heartbeat/registry-refresh，容量不足会导致请求排队、甚至『获取连接超时』。
    // 调大到 300-500 可避免连接池耗尽，但占用更多内存/系统句柄。参考计算：
    // (预期并发请求数) <= maxTotalConnections < (预期并发数 * Eureka 集群节点数)。
    // 调用链：EurekaClientFactoryBuilder.withMaxTotalConnections() → EurekaJerseyClientImpl/
    //   JerseyEurekaHttpClientFactory 设置到 ThreadSafeClientConnManager。
    // 配置项 eurekaServer.maxTotalConnections。
    // 坑：与下面 getEurekaServerTotalConnectionsPerHost() 的关系——总数是全局约束，单 host
    //   上限是每台服务器的约束；两个同时生效，取小者适用。
    int getEurekaServerTotalConnections();

    /**
     * Gets the total number of connections that is allowed from eureka client
     * to a eureka server host.
     *
     * @return total number of allowed connections from eureka client to a
     *         eureka server.
     */
    // 【单 host 连接数上限 · 默认 50】Apache HttpClient 连接管理器对『单个 Eureka 服务端
    // 主机』(由 IP:port 标识)的连接上限。若有 5 台 Eureka 服务端、每台最多 50 个连接，理论全局
    // 最多 250 个。此值调小(如 20)会在单个服务端过载时快速溢出，导致其他请求等待；调大(如 100)
    // 可充分利用单个服务端的并发能力。调整场景：当应用的心跳/注册并发度很高、单台 Eureka 节点
    // 总是『连接数满』且丢弃请求时，尝试调大本值。
    // 调用链：同上 EurekaClientFactoryBuilder.withMaxConnectionsPerHost(本值) →
    //   设置给 ThreadSafeClientConnManager 的 setDefaultMaxPerRoute()。
    // 配置项 eurekaServer.maxConnectionsPerHost。
    // 坑：此值受制于 getEurekaServerTotalConnections()——若 maxTotalConnections=200 而
    //   maxConnectionsPerHost=50，实际单 host 能用的可能 < 50(取决于其他 host 的占用)。
    int getEurekaServerTotalConnectionsPerHost();

    /**
     * Gets the URL context to be used to construct the <em>service url</em> to
     * contact eureka server when the list of eureka servers come from the
     * DNS.This information is not required if the contract returns the service
     * urls by implementing {@link #getEurekaServerServiceUrls(String)}.
     *
     * <p>
     * The DNS mechanism is used when
     * {@link #shouldUseDnsForFetchingServiceUrls()} is set to <em>true</em> and
     * the eureka client expects the DNS to configured a certain way so that it
     * can fetch changing eureka servers dynamically.
     * </p>
     *
     * <p>
     * <em>The changes are effective at runtime.</em>
     * </p>
     *
     * @return the string indicating the context {@link java.net.URI} of the eureka
     *         server.
     */
    // 【Eureka Server 上下文路径 · DNS/Config 模式共用】拼接进 Eureka Server URL 中的「路径前缀」部分，
    // 例如 /eureka 或 /eureka/v2。最终 Server URL 形如 http://hostname:port{此值}/apps。
    // 此值为 null 时会被忽略(URL 中不含路径)；为 "/" 时不另外拼 /；非空且不以 / 开头会被自动补前 /。
    // 调用链：EndpointUtils.getServiceUrlsFromDNS 和 getServiceUrlsFromConfig 等地方都会拼接此值到
    //   HTTP URL 中，例如 sb.append(clientConfig.getEurekaServerURLContext());
    //   LegacyClusterResolver 在构建 DnsTxtRecordClusterResolver 时也会传入此值。
    // 配置项 eurekaServer.context (主)、context (回源，向后兼容)。
    // 坑：拼接时要注意 URL 格式，通常应为 /eureka 或 /eureka/v2 这样的格式。
    String getEurekaServerURLContext();

    /**
     * Gets the port to be used to construct the <em>service url</em> to contact
     * eureka server when the list of eureka servers come from the DNS.This
     * information is not required if the contract returns the service urls by
     * implementing {@link #getEurekaServerServiceUrls(String)}.
     *
     * <p>
     * The DNS mechanism is used when
     * {@link #shouldUseDnsForFetchingServiceUrls()} is set to <em>true</em> and
     * the eureka client expects the DNS to configured a certain way so that it
     * can fetch changing eureka servers dynamically.
     * </p>
     *
     * <p>
     * <em>The changes are effective at runtime.</em>
     * </p>
     *
     * @return the string indicating the port where the eureka server is
     *         listening.
     */
    // 【Eureka Server 监听端口 · DNS 模式必填、Config 模式可选】DNS 寻址方式下，解析的 Server 地址
    // 所用的端口号(如 8080、8443 等)；Config 静态方式下如果 getEurekaServerServiceUrls 返回的 URL
    // 已含完整端口，此值可不用。返回值是 String 而非 int，需要调用方自行转换(如 Integer.parseInt())。
    // 调用链：LegacyClusterResolver.LegacyClusterResolverFactory.createClusterResolver ——
    //   Integer.parseInt(clientConfig.getEurekaServerPort()) 传给 DnsTxtRecordClusterResolver;
    //   EndpointUtils.getServiceUrlsFromDNS 中拼接到 URL 的 "sb.append(':').append(port)";
    //   ConfigClusterResolver 也类似调用 Integer.parseInt(clientConfig.getEurekaServerPort())。
    // 配置项 eurekaServer.port (主)、port (回源)。
    // 坑：是 String 而不是 int，且从配置读出时可能为 null；转换时要加 null 检查。
    String getEurekaServerPort();

    /**
     * Gets the DNS name to be queried to get the list of eureka servers.This
     * information is not required if the contract returns the service urls by
     * implementing {@link #getEurekaServerServiceUrls(String)}.
     *
     * <p>
     * The DNS mechanism is used when
     * {@link #shouldUseDnsForFetchingServiceUrls()} is set to <em>true</em> and
     * the eureka client expects the DNS to configured a certain way so that it
     * can fetch changing eureka servers dynamically.
     * </p>
     *
     * <p>
     * <em>The changes are effective at runtime.</em>
     * </p>
     *
     * @return the string indicating the DNS name to be queried for eureka
     *         servers.
     */
    // 【Eureka Server DNS 域名 · DNS 寻址模式必填】当 shouldUseDnsForFetchingServiceUrls=true 时，
    // 用此 DNS 域名去查询可用的 Eureka Server 列表。DNS 查询会在前面拼接上 region 和 "txt." 前缀，
    // 如 region=us-east-1、此值=eureka.example.com，则查询的 DNS 为 "txt.us-east-1.eureka.example.com"。
    // 无此值(为 null) 时，DNS 方式无法工作，会降级到静态配置方式。
    // 调用链：LegacyClusterResolver.LegacyClusterResolverFactory.createClusterResolver ——
    //   拼接字符串 String discoveryDnsName = "txt." + myRegion + '.' + clientConfig.getEurekaServerDNSName();
    //   ConfigClusterResolver.getDiscoveryDnsName 也类似拼接。
    // 配置项 eurekaServer.domainName，兼容旧 domainName。
    // 坑：一定要确保 DNS 服务器已配置好 txt 记录，否则解析会失败。
    String getEurekaServerDNSName();

    /**
     * Indicates whether the eureka client should use the DNS mechanism to fetch
     * a list of eureka servers to talk to. When the DNS name is updated to have
     * additional servers, that information is used immediately after the eureka
     * client polls for that information as specified in
     * {@link #getEurekaServiceUrlPollIntervalSeconds()}.
     *
     * <p>
     * Alternatively, the service urls can be returned
     * {@link #getEurekaServerServiceUrls(String)}, but the users should implement
     * their own mechanism to return the updated list in case of changes.
     * </p>
     *
     * <p>
     * <em>The changes are effective at runtime.</em>
     * </p>
     *
     * @return true if the DNS mechanism should be used for fetching urls, false otherwise.
     */
    // 【寻址方式选择开关 · 默认关闭】客户端获取 Eureka Server 地址列表时，是否采用 DNS 动态解析方式
    // (对应 true) 还是配置文件静态列表方式(对应 false)。当此开关为 true 时，下面的三个配置
    // (getEurekaServerDNSName / getEurekaServerPort / getEurekaServerURLContext) 才会被激活，
    // 客户端通过拼接这三个字段生成访问 URL；为 false 时，直接走 getEurekaServerServiceUrls(zone) 返回的静态列表。
    // DNS 方式优势：Server 变更时无需重启客户端(仅需更新 DNS 记录)，更灵活；劣势是对 DNS 基础设施有依赖。
    // 静态列表方式优势：无额外依赖，配置清晰；劣势是Server 变更需重启客户端。
    // AWS 环境常用 DNS 方式；自建数据中心通常静态配置。
    // 调用链：LegacyClusterResolver.LegacyClusterResolverFactory.createClusterResolver 和
    //   EndpointUtils.getDiscoveryServiceUrls 的分支判断点——决定走 getServiceUrlsFromDNS 还是
    //   getServiceUrlsFromConfig。
    // 配置项 shouldUseDns。
    boolean shouldUseDnsForFetchingServiceUrls();

    /**
     * Indicates whether or not this instance should register its information
     * with eureka server for discovery by others.
     *
     * <p>
     * In some cases, you do not want your instances to be discovered whereas
     * you just want do discover other instances.
     * </p>
     *
     * @return true if this instance should register with eureka, false
     *         otherwise
     */
    // 【是否注册开关 · 默认 true】控制该应用实例是否主动向 Eureka 服务端注册自身信息，使得其他客户端
    // 可以发现它。本开关对客户端的影响是"全局性的"：若为 false，下列功能全部禁用：
    //   - 心跳续约（HeartbeatThread 不启动）
    //   - 实例信息复制（InstanceInfoReplicator 不启动）
    //   - 关联的两个执行器线程池不创建（heartbeatExecutor / 其他复制相关池）
    // 典型场景："纯查询客户端"或"内部工具服务"只需要发现别人，不需要被发现，可设为 false 优化资源消耗。
    // 若 shouldRegisterWithEureka 和 shouldFetchRegistry 都为 false，则客户端不创建任何后台线程，
    // 仅作为一个"空壳"以兼容依赖 DiscoveryManager 的遗留代码。
    // 配置项 registration.enabled。调用链：DiscoveryClient 主构造 → initScheduledTasks(本值决定注册相关
    //   线程启动与否) / HeartbeatThread / InstanceInfoReplicator 的启用。
    boolean shouldRegisterWithEureka();

    /**
     * Indicates whether the client should explicitly unregister itself from the remote server
     * on client shutdown.
     *
     * @return true if this instance should unregister with eureka on client shutdown, false otherwise
     */
    default boolean shouldUnregisterOnShutdown() {
        return true;
    }

    /**
     * Indicates whether or not this instance should try to use the eureka
     * server in the same zone for latency and/or other reason.
     *
     * <p>
     * Ideally eureka clients are configured to talk to servers in the same zone
     * </p>
     *
     * <p>
     * <em>The changes are effective at runtime at the next registry fetch cycle as specified by
     * {@link #getRegistryFetchIntervalSeconds()}</em>
     * </p>
     *
     * @return true if the eureka client should prefer the server in the same
     *         zone, false otherwise.
     */
    // 【同 Zone 优先开关 · 默认开启】控制客户端是否优先向与自己相同 availability zone 的 Eureka
    // Server 发起请求(获取注册表、心跳、注册等)。当此值为 true 时，客户端会在 Server 列表中优先
    // 筛选出与本实例同 zone 的 Server；为 false 时，优先选「非同 zone」的 Server(用于特殊的
    // 跨 zone 或负载均衡场景)。
    // why？同 zone 网络延迟更低、跨 zone 流量通常需付费，多 AZ 部署中这是「就近原则」的体现。
    // 调大/调小(开关)/影响：true(默认) 时 DiscoveryClient 启动后，通过 ZoneAffinityClusterResolver
    // 对 Server 列表进行就近排序，优先返回同 zone Server；false 时此排序不生效，Server 顺序随机或按配置。
    // 场景：AWS 多 AZ 部署、金融云等收费网络环境，建议保持 true；单 zone/内网环境可设为 false 简化部署。
    // 调用链：LegacyClusterResolver.LegacyClusterResolverFactory.createClusterResolver ——
    //   newResolver = new ZoneAffinityClusterResolver(newResolver, myZone, clientConfig.shouldPreferSameZoneEureka(), randomizer);
    //   EndpointUtils.getDiscoveryServiceUrls —— getServiceUrlsFromDNS/getServiceUrlsFromConfig 都传入此参数;
    //   ConfigClusterResolver.getServiceUrlsMapFromConfig 也用此值进行 zone 过滤。
    // 配置项 preferSameZone。
    boolean shouldPreferSameZoneEureka();

    /**
     * Indicates whether server can redirect a client request to a backup server/cluster.
     * If set to false, the server will handle the request directly, If set to true, it may
     * send HTTP redirect to the client, with a new server location.
     *
     * @return true if HTTP redirects are allowed
     */
    // 【HTTP 重定向开关 · 默认 false(不允许)】若设为 true，客户端允许接收服务端的 HTTP
    // 重定向响应(3xx)，并自动跟随到新的 URL；若为 false，客户端不跟随重定向、直接返回 3xx
    // 状态码给调用方。Eureka 服务端可在负载均衡场景下发送『X-Discovery-AllowRedirect: true
    // 』响应头提示客户端是否允许重定向。allowRedirects=true 适用于【多个 Eureka 集群/故障转移
    // 场景】——服务端返回 302+Location=备集群 URL，客户端自动转向；但同时引入额外延迟(二次
    // 连接)。allowRedirects=false 时，DiscoveryClient 需【手动处理重定向】(见源码
    // makeRemoteCall 对 3xx 的处理)。默认 false 是保守做法，避免自动跟随导致请求路由混乱。
    // 调用链：EurekaClientFactoryBuilder.withClientConfig() → withAllowRedirect(本值) →
    //   JerseyEurekaHttpClientFactory.build() 中，若本值为 true，将 HTTP_X_DISCOVERY_ALLOW_REDIRECT
    //   添加到请求头(值为 'true')，服务端据此决定是否下发重定向。
    // 配置项 allowRedirects。
    // 坑：此开关【不是 HTTP 客户端自动跟随重定向】的意思，而是『告诉服务端：我允许被重定向』的
    //   意思——服务端收到此头后才会主动返回重定向响应；否则服务端会直接处理请求。
    boolean allowRedirects();

    /**
     * Indicates whether to log differences between the eureka server and the
     * eureka client in terms of registry information.
     *
     * <p>
     * Eureka client tries to retrieve only delta changes from eureka server to
     * minimize network traffic. After receiving the deltas, eureka client
     * reconciles the information from the server to verify it has not missed
     * out some information. Reconciliation failures could happen when the
     * client has had network issues communicating to server.If the
     * reconciliation fails, eureka client gets the full registry information.
     * </p>
     *
     * <p>
     * While getting the full registry information, the eureka client can log
     * the differences between the client and the server and this setting
     * controls that.
     * </p>
     * <p>
     * <em>The changes are effective at runtime at the next registry fetch cycle as specified by
     * {@link #getRegistryFetchIntervalSeconds()}</em>
     * </p>
     *
     * @return true if the eureka client should log delta differences in the
     *         case of reconciliation failure.
     */
    // 【打印增量差异日志 · 默认 false】此开关仅控制日志输出，不改变功能逻辑，但触发修复动作。
    // 当本地与服务端 hashcode 不一致时（增量合并出错的信号），client 会调 reconcileAndLogDifference()
    //   拉一遍全量、计算差异、打详细日志。此开关为 true 时，即使 hashcode 相等也强制打印日志，用于排查幽灵 bug。
    // 主要场景：
    //   1. 怀疑增量合并有隐藏异常（日志没输出过）→ 临时改为 true，看 INFO/WARN 日志里是否有"Reconcile hashcodes do not match"之类
    //   2. 线上流量异常、某些实例无故查不到，需要对比客户端和服务端的注册表数据
    // 调用链：DiscoveryClient.getAndUpdateDelta →
    //   if (hashcode 不等 OR shouldLogDeltaDiff) reconcileAndLogDifference()。
    // 配置项 printDeltaFullDiff。注意：为 true 会增加日志 I/O，生产环境不推荐长期开启。
    boolean shouldLogDeltaDiff();

    /**
     * Indicates whether the eureka client should disable fetching of delta and
     * should rather resort to getting the full registry information.
     *
     * <p>
     * Note that the delta fetches can reduce the traffic tremendously, because
     * the rate of change with the eureka server is normally much lower than the
     * rate of fetches.
     * </p>
     * <p>
     * <em>The changes are effective at runtime at the next registry fetch cycle as specified by
     * {@link #getRegistryFetchIntervalSeconds()}</em>
     * </p>
     *
     * @return true to enable fetching delta information for registry, false to
     *         get the full registry.
     */
    // 【禁用增量拉取 · 默认 false】此开关影响拉取策略：false(默认) → 尽量拉"增量" ； true → 始终拉"全量"。
    // 背景：增量(delta) = 最近 3 分钟的变更记录，体积小；全量(full) = 所有应用的所有实例，体积大。
    //   默认策略首次全量，之后增量（增量失败或 hashcode 校验不一致时自动回退全量）。
    // 何时改为 true？
    //   1. 怀疑增量合并出错，临时调试用（增量数据丢失/乱序可能导致本地缓存不一致）
    //   2. 老版本 Eureka 服务端 delta 功能有 bug，强制全量规避
    //   3. 业务逻辑严格要求绝对最新的完整注册表（但大多数场景不需要）
    // 调大代价：每次拉取传输数据量增加 5~10 倍，网络带宽 + 反序列化成本明显上升，服务端 GC 压力增加。
    // 调用链：DiscoveryClient.fetchRegistry → if (shouldDisableDelta) getAndStoreFullRegistry() else getAndUpdateDelta()。
    // 配置项 disableDelta。相关：shouldLogDeltaDiff (打印增量差异日志)、getRegistryRefreshSingleVipAddress (单 VIP 模式)。
    boolean shouldDisableDelta();

    /**
     * Comma separated list of regions for which the eureka registry information will be fetched. It is mandatory to
     * define the availability zones for each of these regions as returned by {@link #getAvailabilityZones(String)}.
     * Failing to do so, will result in failure of discovery client startup.
     *
     * @return Comma separated list of regions for which the eureka registry information will be fetched.
     * <code>null</code> if no remote region has to be fetched.
     */
    @Nullable
    // 【远程区域拉取列表 · 默认 null】逗号分隔的区域名列表，客户端将同时拉这些远程区域的实例数据，
    // 除了本地区域(localRegion)的注册表。
    // 典型场景：AWS 跨区域 / 多数据中心部署。如 fetchRegistryForRemoteRegions=us-west-2,eu-west-1
    // 实现细节：
    //   - 由 refreshRegistry() 定时检查此值的变化，若变更则更新 remoteRegionsToFetch 和 remoteRegionsRef 两个变量
    //   - 拉取时给服务端的请求会带上"localRegion + 这些远程区域"，服务端返回联合数据（但增量不支持跨区域）
    //   - 本地维护 remoteRegionVsApps 这个 Map 来分别存储每个远程区域的注册表数据
    // 限制与坑：
    //   - 必须在 eureka.properties 中提前定义每个区域的可用区列表，如 eureka.us-west-2.availabilityZones=us-west-2a,us-west-2b
    //   - 配置不当会导致客户端启动失败
    //   - 仅在全量拉取时生效，增量拉取会被忽略
    //   - 与 shouldDisableDelta 或 getRegistryRefreshSingleVipAddress 叠加使用时行为复杂，易出现边界 case
    // 调用链：DiscoveryClient 构造器(初始化 remoteRegionsToFetch)、refreshRegistry()(监听配置变化) →
    //   getAndStoreFullRegistry/getAndUpdateDelta 都用 remoteRegionsRef 来决定拉哪些区域的实例。
    // 配置项 fetchRemoteRegionsRegistry。
    String fetchRegistryForRemoteRegions();

    /**
     * Gets the region (used in AWS datacenters) where this instance resides.
     *
     * @return AWS region where this instance resides.
     */
    // 【本实例所在的 AWS Region · 默认 us-east-1】用来标识客户端/实例所在的地理区域(AWS 概念)。
    // 在多 region 部署场景下，Eureka 用 region 作为「一级划分」，region 下再分 AZ(availabilityZone)。
    // 本值在以下场景被用到：① DNS 寻址时拼接到域名前缀("txt.{region}.eureka.example.com")；
    // ② 作为注册信息的一部分上报到服务端；③ 远程 region 配置(fetchRegistryForRemoteRegions) 时过滤数据。
    // 非 AWS 环境可视需要取值为「业务地区代称」如 "shanghai"、"beijing" 等；单机房部署可默认不改。
    // 调用链：DiscoveryClient.<init> 中 logger.info("Initializing Eureka in region {}", clientConfig.getRegion());
    //   LegacyClusterResolver.LegacyClusterResolverFactory.<init> 用 this.myRegion = clientConfig.getRegion() 保存;
    //   PropertyBasedAzToRegionMapper.getAvailabilityZones 用 clientConfig.getRegion() 查询本 region 的 AZ;
    //   EndpointUtils.getZoneBasedDiscoveryUrlsFromRegion 同样用 region 查询 zone 列表。
    // 配置项 region (主)、eureka.region (备用)。
    String getRegion();

    /**
     * Gets the list of availability zones (used in AWS data centers) for the
     * region in which this instance resides.
     *
     * <p>
     * <em>The changes are effective at runtime at the next registry fetch cycle as specified by
     * {@link #getRegistryFetchIntervalSeconds()}</em>
     * </p>
     * @param region the region where this instance is deployed.
     *
     * @return the list of available zones accessible by this instance.
     */
    // 【指定 Region 下的可用区列表 · 默认返回 ["defaultZone"]】为给定的 region 配置该 region
    // 下有哪些 availability zones (AZ)。例如 AWS us-east-1 下可能有 us-east-1a、us-east-1b、us-east-1c 等。
    // 本值与「同 zone 优先」策略(shouldPreferSameZoneEureka) 配合使用：客户端会优先向同一 AZ 的
    // Server 发请求，降低网络延迟和跨 AZ 流量费用。调用此方法需指定 region 参数；若 region 不存在
    // 配置，则返回默认值(一个元素的数组: ["defaultZone"])。
    // 多 AZ 部署时，建议明确配置本值(如 "us-east-1a,us-east-1b,us-east-1c")，
    // 并确保 getEurekaServerServiceUrls(zone) 也针对各个 zone 有相应配置。
    // 调用链：DiscoveryClient.<init> 中 String[] availZones = staticClientConfig.getAvailabilityZones(staticClientConfig.getRegion());
    //   PropertyBasedAzToRegionMapper.getAvailabilityZones(region) —— Arrays.asList(clientConfig.getAvailabilityZones(region));
    //   EndpointUtils.getServiceUrlsFromDNS / getServiceUrlsFromConfig 都会遍历 availableZones 做同 zone 优先处理。
    // 配置项 {region}.availabilityZones，如 us-east-1.availabilityZones=us-east-1a,us-east-1b。
    // 坑：配置值逗号分隔，多余空格会被正则 "\\s*,\\s*" 清理；返回结果是数组，调用方遍历时注意边界。
    String[] getAvailabilityZones(String region);

    /**
     * Gets the list of fully qualified {@link java.net.URL}s to communicate with eureka
     * server.
     *
     * <p>
     * Typically the eureka server {@link java.net.URL}s carry protocol,host,port,context
     * and version information if any.
     * <code>Example: http://ec2-256-156-243-129.compute-1.amazonaws.com:7001/eureka/v2/</code>
     * <p>
     *
     * <p>
     * <em>The changes are effective at runtime at the next service url refresh cycle as specified by
     * {@link #getEurekaServiceUrlPollIntervalSeconds()}</em>
     * </p>
     * @param myZone the zone in which the instance is deployed.
     *
     * @return the list of eureka server service urls for eureka clients to talk
     *         to.
     */
    List<String> getEurekaServerServiceUrls(String myZone);

    /**
     * Indicates whether to get the <em>applications</em> after filtering the
     * applications for instances with only {@link com.netflix.appinfo.InstanceInfo.InstanceStatus#UP} states.
     *
     * <p>
     * <em>The changes are effective at runtime at the next registry fetch cycle as specified by
     * {@link #getRegistryFetchIntervalSeconds()}</em>
     * </p>
     *
     * @return true to filter, false otherwise.
     */
    // 【实例状态过滤开关 · 默认开启】控制本客户端从 Eureka 获取到的应用实例列表中，是否只返回
    // 状态为 UP 的实例(对应 true)，还是返回所有实例包括 DOWN/STARTING/OUT_OF_SERVICE 等(对应 false)。
    // 当此值为 true 时，DiscoveryClient 拿到注册表后会对每个应用(application)调用 shuffleInstances(true),
    // 该方法会过滤并只保留 InstanceStatus.UP 的实例；为 false 时保留全部状态的实例。
    // why？通常只想向「健康运行」的实例(UP 状态) 发送请求，DOWN/OUT_OF_SERVICE 的实例不应被调用。
    // 为 false 的场景是某些运维需要在客户端看到全量实例信息(包括故障节点)，便于诊断。
    // 调大/调小(开关)/影响：true(默认) 时 shuffle 会过滤掉非 UP 实例，客户端的 Load Balancer
    // 只看得到 UP 实例；false 时暴露全量实例，Risk 是客户端可能向故障节点发请求。
    // 场景：正常生产环境保持 true 保证只请求健康实例；开发/测试/灰度验证环境可设为 false 查看全量状态。
    // 调用链：DiscoveryClient.getApplications()/refreshRegistry/processDelta 等地方都会调用
    //   applications.shuffleInstances(clientConfig.shouldFilterOnlyUpInstances()) 对实例列表进行过滤;
    //   Application._shuffleAndStoreInstances 中根据此参数决定是否过滤。
    // 配置项 shouldFilterOnlyUpInstances。
    boolean shouldFilterOnlyUpInstances();

    /**
     * Indicates how much time (in seconds) that the HTTP connections to eureka
     * server can stay idle before it can be closed.
     *
     * <p>
     * In the AWS environment, it is recommended that the values is 30 seconds
     * or less, since the firewall cleans up the connection information after a
     * few mins leaving the connection hanging in limbo
     * </p>
     *
     * @return time in seconds the connections to eureka can stay idle before it
     *         can be closed.
     */
    // 【空闲连接回收超时 · 默认 45s】HttpClient 连接池内闲置的连接在多久(秒)未被使用后、就
    // 会被后台清理线程关闭并移出连接池。AWS 防火墙通常在 1-5 分钟后清理半开连接，所以 Eureka
    // 推荐此值 <= 30s 以主动关闭陈旧连接、避免防火墙 RST 异常。调大到 60s+ 会让长期闲置的
    // TCP 连接占着茅坑；调小到 10s 会导致频繁重建连接、增加 CPU 和网络开销。一般 30-45s 较均衡。
    // 调用链：ApacheHttpClientConnectionCleaner 后台线程(每 30s 运行一次)调用
    //   HttpConnectionManager.closeIdleConnections(本值, TimeUnit.SECONDS)，扫描池内超过
    //   本值秒未活动的连接并关闭。由 EurekaClientFactoryBuilder.withConnectionIdleTimeout()
    //   → JerseyEurekaHttpClientFactory 初始化时创建 ApacheHttpClientConnectionCleaner。
    // 配置项 eurekaserver.connectionIdleTimeoutInSeconds(注意名称中 eurekaserver 全小写，
    //   与其他配置不一致——『向后兼容性问题』)。
    int getEurekaConnectionIdleTimeoutSeconds();

    /**
     * Indicates whether this client should fetch eureka registry information from eureka server.
     *
     * @return {@code true} if registry information has to be fetched, {@code false} otherwise.
     */
    // 【是否拉取注册表 · 默认 true】此开关控制 DiscoveryClient 是否要启用"拉取注册表"这一核心功能。
    // 典型用途：大多数微服务需要 true；某些"只发不收"的业务方（如网关/中间件专门提供服务而不调用其他服务）
    //   可设为 false 来节省网络开销和启动时间（但仍可通过 register() 向服务端注册自己）。
    // 若为 false，客户端不会启动 cacheRefreshTask 定时任务、initScheduledTasks 中条件判断会跳过，
    //   getApplications()/getInstances() 等查询方法将始终返回空或本地初始化的缺省值；
    // 同时，构造函数会检查 shouldRegisterWithEureka 和 shouldFetchRegistry 至少有一个为 true，否则警告。
    // 配置项 shouldFetchRegistry。坑：虽然大小写混合，但在属性文件中应写 shouldFetchRegistry=false。
    boolean shouldFetchRegistry();

    /**
     * If set to true, the {@link EurekaClient} initialization should throw an exception at constructor time
     * if the initial fetch of eureka registry information from the remote servers is unsuccessful.
     *
     * Note that if {@link #shouldFetchRegistry()} is set to false, then this config is a no-op.
     *
     * @return true or false for whether the client initialization should enforce an initial fetch.
     */
    default boolean shouldEnforceFetchRegistryAtInit() {
        return false;
    }

    /**
     * Indicates whether the client is only interested in the registry information for a single VIP.
     *
     * @return the address of the VIP (name:port).
     * <code>null</code> if single VIP interest is not present.
     */
    @Nullable
    // 【单 VIP 模式 · 默认 null】若设置为"APP-NAME:PORT"形式，客户端只关注该 VIP 下的实例，而不是所有应用。
    // 使用场景（罕见）：
    //   1. 某个微服务只调用特定的上游服务，网络带宽极其珍贵 → 配置此项，缩小注册表范围到单个 VIP
    //   2. 网关/反向代理只需维护一份目标服务的实例列表 → 减少拉取和本地缓存数据量
    // 副作用与限制：
    //   - 一旦设置，fetchRegistry() 会"不自动选择增量"，转而强制全量拉取该 VIP 对应的实例
    //   - getApplications()/getInstances() 的结果集只包含该 VIP 的数据，无法查询其他服务
    //   - 与 shouldDisableDelta 互斥效果：设置此项后，增量/全量策略自动降级为全量
    //   - 不能查询远程区域（remoteRegion）的实例
    // 调用链：DiscoveryClient.getApplications/getAndStoreFullRegistry/reconcileAndLogDifference 都检查此值，
    //   若非 null 则调 eurekaTransport.queryClient.getVip(vipAddress) 而非 getApplications()。
    // 配置项 registryRefreshSingleVipAddress，形式如 "MY-SERVICE:8080"。
    String getRegistryRefreshSingleVipAddress();

    /**
     * The thread pool size for the heartbeatExecutor to initialise with
     *
     * @return the heartbeatExecutor thread pool size
     */
    // 【心跳执行器线程池大小 · 默认 5】HeartbeatThread(续约任务)提交到的 ThreadPoolExecutor 的核心
    // 线程数和最大线程数（两者相同）。该线程池采用 SynchronousQueue（同步队列），意味着：
    //   - 每个进来的心跳任务都必须被一个线程"直接"接收，否则拒绝（RejectedExecutionException）
    //   - 线程数可以动态增长到 maxPoolSize(本值)，超过则拒绝
    //   - 新建的线程 60s 空闲后自动销毁（回到 corePoolSize=1）
    // 调大该值(如 10)：多个心跳任务可并发执行，提升吞吐量；调小会增加拒绝概率，心跳延迟。
    // 注意与 getCacheRefreshExecutorThreadPoolSize() 区分，两者独立——心跳和注册表拉取各自一个线程池。
    // 与 getHeartbeatExecutorExponentialBackOffBound() 成对：心跳超时时下次间隔
    //   = renewalIntervalInSecs(默认30s) × backoffBound 的指数增长。
    // 配置项 client.heartbeat.threadPoolSize。调用链：DiscoveryClient 主构造 →
    //   new ThreadPoolExecutor(1, 本值, 0, TimeUnit.SECONDS, SynchronousQueue, ...)。
    int getHeartbeatExecutorThreadPoolSize();

    /**
     * Heartbeat executor exponential back off related property.
     * It is a maximum multiplier value for retry delay, in case where a sequence of timeouts
     * occurred.
     *
     * @return maximum multiplier value for retry delay
     */
    // 【心跳指数退避上界 · 默认 10】当心跳任务执行超时（默认超时 = renewalIntervalInSecs = 30s）时，
    // TimedSupervisorTask 控制下次调度的延迟增长倍数的"上限乘数"。
    // 计算公式：maxDelay = timeoutMillis × expBackOffBound = (30s × 本值) = 30s × 10 = 300s
    // 工作过程：首次超时 → delay 翻倍 30s→60s，第二次超时再翻倍 60s→120s，...，最大累积到 300s 停止翻倍。
    // 目的：网络抖动/服务端故障时自动放缓心跳频率，避免心跳请求堆积和线程池爆满；恢复后(执行成功)立刻回到 30s。
    // 为什么需要退避？心跳超时通常意味着网络不通/服务端处理缓慢，继续以 30s 频率狂轰只会加重压力；
    // 退避让客户端"退缩"给服务端恢复机会。
    // 调大(如 20)：最大延迟更长(600s)，在长期故障时心跳更稀疏；调小(如 3)：最大 90s，恢复更快但故障时压力更大。
    // 与 getHeartbeatExecutorThreadPoolSize() 成对：线程池大小控制并发度，退避上界控制时间范围。
    // 配置项 client.heartbeat.exponentialBackOffBound。调用链：DiscoveryClient.initScheduledTasks →
    //   new TimedSupervisorTask(..., expBackOffBound, heartbeatThread)。
    int getHeartbeatExecutorExponentialBackOffBound();

    /**
     * The thread pool size for the cacheRefreshExecutor to initialise with
     *
     * @return the cacheRefreshExecutor thread pool size
     */
    // 【注册表刷新执行器线程池大小 · 默认 5】CacheRefreshThread(拉取服务端注册表)提交到的
    // ThreadPoolExecutor 的核心线程数和最大线程数(两者相同)。结构与用途与 getHeartbeatExecutorThreadPoolSize()
    // 完全相同，唯一区别是处理的是"注册表拉取任务"而非"心跳续约任务"。
    // 该线程池也采用 SynchronousQueue，每个注册表拉取任务都必须被线程直接接收，否则拒绝。
    // 调大该值(如 10)：若注册表拉取任务耗时长(网络慢/注册表超大)可提升并发；调小会增加拒绝风险。
    // 与 getHeartbeatExecutorThreadPoolSize() 的关键区别：独立的两个线程池，互不影响——
    // 即使心跳线程饱和也不会阻塞注册表拉取，反之亦然。
    // 与 getCacheRefreshExecutorExponentialBackOffBound() 成对：注册表拉取超时时下次间隔的指数增长。
    // 配置项 client.cacheRefresh.threadPoolSize。调用链：DiscoveryClient 主构造 →
    //   new ThreadPoolExecutor(1, 本值, 0, TimeUnit.SECONDS, SynchronousQueue, ...)。
    int getCacheRefreshExecutorThreadPoolSize();

    /**
     * Cache refresh executor exponential back off related property.
     * It is a maximum multiplier value for retry delay, in case where a sequence of timeouts
     * occurred.
     *
     * @return maximum multiplier value for retry delay
     */
    // 【注册表拉取指数退避上界 · 默认 10】当注册表拉取任务执行超时（默认超时 = registryFetchIntervalSeconds
    // = 30s）时，TimedSupervisorTask 控制下次调度延迟增长倍数的"上限乘数"。
    // 计算公式：maxDelay = timeoutMillis × expBackOffBound = (30s × 本值) = 30s × 10 = 300s
    // 工作过程完全同 getHeartbeatExecutorExponentialBackOffBound()：超时后翻倍递增，直到达到 300s 上限；
    // 成功执行则立刻恢复为 30s。
    // 目的：同样的退避思想——当服务端注册表拉取频繁超时(可能服务端负载过高或网络不通)，自动放缓拉取频率，
    // 从 30s → 60s → 120s → ... → 最大 300s，给服务端恢复窗口；恢复后自动加速回 30s。
    // 为什么需要退避？频繁超时的拉取请求对服务端和客户端都是浪费，退避能减轻双方压力。
    // 调大(如 20)：最大延迟 600s，长期故障时拉取极稀疏(缺点：故障恢复时本地缓存陈旧)；
    // 调小(如 3)：最大 90s，快速恢复但故障时压力大。
    // 与 getCacheRefreshExecutorThreadPoolSize() 成对：线程池控制并发，退避上界控制延迟范围。
    // 配置项 client.cacheRefresh.exponentialBackOffBound。调用链：DiscoveryClient.initScheduledTasks →
    //   new TimedSupervisorTask(..., expBackOffBound, cacheRefreshThread)。
    int getCacheRefreshExecutorExponentialBackOffBound();

    /**
     * Get a replacement string for Dollar sign <code>$</code> during serializing/deserializing information in eureka server.
     *
     * @return Replacement string for Dollar sign <code>$</code>.
     */
    // 【XStream 编码·$ 字符转义 · 默认"_-"】
    // XML 序列化时，某些实例字段名含有 $ 符号(如成本分析字段)，直接写入 XML 标签会导致解析失败；
    // 通过此配置替换 $ 为字符串"_-"，使 XML 合法。反序列化时自动转换回 $，对业务透明。
    // 为什么需要转义：XStream 用字段名作 XML 元素名，$ 在 XML 命名空间里违规。
    // 调用链：XmlXStream.initializeNameCoder(本值+getEscapeCharReplacement) → new XmlFriendlyNameCoder →
    //   DiscoveryClient.getClientCodec() 序列化 InstanceInfo 时应用。
    // 配置项 dollarReplacement；注意要与 getEscapeCharReplacement() 配合使用。
    String getDollarReplacement();

    /**
     * Get a replacement string for underscore sign <code>_</code> during serializing/deserializing information in eureka server.
     *
     * @return Replacement string for underscore sign <code>_</code>.
     */
    // 【XStream 编码·_ 字符转义 · 默认"__"】
    // XML/JSON 序列化时，下划线 _ 是 XmlFriendlyNameCoder 的转义操作符；如字段本身有 _，需先转义为"__"，
    // 避免与被 $ 转义后产生的"_-"混淆(如 $ 被转为"_-"，而原本的 _ 需是"__")。
    // 为什么二级转义：XStream 用 _ 作分隔符进行字符映射，所以 _ 本身必须先转义，才能用于转义其他字符。
    // 调用链：XmlXStream.initializeNameCoder(getDollarReplacement+本值) → XmlFriendlyNameCoder；
    //   也用于 EurekaJacksonCodec 和 KeyFormatter 的 JSON 键名清理。
    // 配置项 escapeCharReplacement；必须与 getDollarReplacement() 一同配置以保持对偶性。
    String getEscapeCharReplacement();

    /**
     * If set to true, local status updates via
     * {@link com.netflix.appinfo.ApplicationInfoManager#setInstanceStatus(com.netflix.appinfo.InstanceInfo.InstanceStatus)}
     * will trigger on-demand (but rate limited) register/updates to remote eureka servers
     *
     * @return true or false for whether local status updates should be updated to remote servers on-demand
     */
    // 【按需状态更新开关 · 默认 true】决定当本实例状态（健康状态/可用状态）发生变化时是否立即向
    // 服务端上报（而不必等下一个 30s 周期）。
    // 为 true 时：状态变更事件（如健康检查从 UP → DOWN）会触发 InstanceInfoReplicator.onDemandUpdate()，
    // 内部用令牌桶限流(默认每分钟最多 4 次，由 burstSize=2 与 replicationIntervalSeconds=30 联合计算)
    // 防止状态抖动导致频繁注册；超限则回退到周期任务。
    // 为 false 时：状态变更不主动上报，只能靠周期复制(默认每 30s)，导致状态变更被服务端看见的延迟最多 30s。
    // 调用链：DiscoveryClient.initScheduledTasks → ApplicationInfoManager.registerStatusChangeListener(
    //   statusChangeListener) → StatusChangeListener.notify() → InstanceInfoReplicator.onDemandUpdate()。
    // 配置项 shouldOnDemandUpdateStatusChange。
    boolean shouldOnDemandUpdateStatusChange();

    /**
     * If set to true, the {@link EurekaClient} initialization should throw an exception at constructor time
     * if an initial registration to the remote servers is unsuccessful.
     *
     * Note that if {@link #shouldRegisterWithEureka()} is set to false, then this config is a no-op
     *
     * @return true or false for whether the client initialization should enforce an initial registration
     */
    default boolean shouldEnforceRegistrationAtInit() {
        return false;
    }

    /**
     * This is a transient config and once the latest codecs are stable, can be removed (as there will only be one)
     *
     * @return the class name of the encoding codec to use for the client. If none set a default codec will be used
     */
    // 【编码器选择·可选 · 默认 null】
    // 指定客户端上报 InstanceInfo 到服务端时的编码器类名(如 com.netflix.discovery.converters.wrappers.CodecWrappers.JacksonJson)。
    // null 表示让系统自动选择(通常为 JSON，对应 EurekaAccept.full)；若明确指定，需确保类在 classpath 存在且与
    // getDecoderName() 互相兼容，否则会导致序列化失败。
    // 应用场景：在多个编码方案(JSON vs XML)间切换时用到，但通常不需改动(保留 null 即可)。
    // 调用链：EurekaClientFactoryBuilder.withEncoder(本值) → 传输层选择 EncoderWrapper。
    // 配置项 encoderName。坑：若值非 null，必须与 getDecoderName() 配套，不能混搭。
    String getEncoderName();

    /**
     * This is a transient config and once the latest codecs are stable, can be removed (as there will only be one)
     *
     * @return the class name of the decoding codec to use for the client. If none set a default codec will be used
     */
    // 【解码器选择·可选 · 默认 null】
    // 指定客户端解析从服务端获取的注册表数据的解码器类名。null 表示自动选择(通常 JSON full 模式)。
    // 与 getEncoderName() 必须匹配，否则序列化/反序列化会报错。也可设为 mini(紧凑) 格式的解码器以减小网络包体积，
    // 但需服务端也支持该格式。
    // 应用场景：在极端网络受限的环境下，可用 JacksonJsonMini 降低流量，代价是稍多 CPU。
    // 调用链：EurekaClientFactoryBuilder.withDecoder(本值, getClientDataAccept) → DecoderWrapper。
    // 配置项 decoderName；与 getEncoderName() 和 getClientDataAccept() 形成三角依赖。
    String getDecoderName();

    /**
     * @return {@link com.netflix.appinfo.EurekaAccept#name()} for client data accept
     */
    // 【注册表数据粒度·默认 full】
    // 控制客户端向服务端声明希望接收的数据格式：full(完整字段)或 compact(紧凑，仅关键字段)。
    // 值为"full"时，接收完整 InstanceInfo；为"compact"时，服务端返回 mini 格式(少字段、体积小)。
    // compact 可减少网络带宽 20-30%，但客户端端仅看到应用关键信息，某些元数据丢失——用于边缘设备/弱网场景。
    // 调用链：EurekaClientFactoryBuilder.withClientConfig → EurekaAccept.fromString(本值) → 设置 HTTP
    //   请求头"X-Eureka-Accept: {full|compact}"发送给服务端，指导下发内容粒度。
    // 配置项 clientDataAccept；值需符合 EurekaAccept.full|compact，否则降级为 full。
    String getClientDataAccept();

    /**
     * To avoid configuration API pollution when trying new/experimental or features or for the migration process,
     * the corresponding configuration can be put into experimental configuration section. Config format is:
     * eureka.experimental.freeFormConfigString
     *
     * @return a property of experimental feature
     */
    // 【实验特性开关·统一入口 · 动态取值】
    // 一个通用接口，用于读取 eureka.experimental.{name} 配置，支持新/实验功能在稳定前的闭包。
    // name 参数为功能标识，如"clientTransportFailFastOnInit"(客户端初始化失败快速抛异常)、
    // "JerseyEurekaHttpClientFactory.useNewBuilder"(使用新版 Jersey HTTP 客户端构造器)。
    // 返回字符串，调用者需自己判断 "true"|"false"|null 来启用/禁用。这样避免频繁新增 getter 方法。
    // 应用场景：当 Netflix 需在不修改接口签名下进行 A/B 测试、灰度发布新算法或过渡旧实现时使用。
    // 调用链：EurekaHttpClients.failFastOnInitCheck(getExperimental("clientTransportFailFastOnInit"))；
    //   JerseyEurekaHttpClientFactory.create(getExperimental("JerseyEurekaHttpClientFactory.useNewBuilder"))。
    // 配置项 eureka.experimental.{任意 name}；通过 String name 参数动态组装键名。
    String getExperimental(String name);

    /**
     * For compatibility, return the transport layer config class
     *
     * @return an instance of {@link EurekaTransportConfig}
     */
    EurekaTransportConfig getTransportConfig();
}
