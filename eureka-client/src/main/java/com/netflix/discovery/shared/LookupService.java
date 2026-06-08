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
package com.netflix.discovery.shared;

import java.util.List;

import com.netflix.appinfo.InstanceInfo;

/**
 * Lookup service for finding active instances.
 *
 * @author Karthik Ranganathan, Greg Kim.
 * @param <T> for backward compatibility

 */
/*
 * 【中文说明 · 对应文档03(拉取注册表→本地缓存) / 09(数据模型·VIP轮询索引)】
 * LookupService 是整个 Eureka「读侧 / 查询」能力的最小公共契约：凡是需要从注册表里
 * "找实例"的角色，都用这一套 API。它刻意与「写侧」(注册/续约，见 LeaseManager) 分开，
 * 是接口隔离的体现——查询方不必关心实例是怎么被写进注册表的。
 *
 * 为什么客户端与服务端共用同一个接口？同一份 API，背后是两种数据源、两种语义：
 *   - 客户端 EurekaClient(由 DiscoveryClient 实现) 查的是"自己缓存的注册表快照"，
 *     即本地 AtomicReference<Applications>(localRegionApps)。读的是"我以为的世界"，
 *     断网后仍能做服务发现，靠的就是这份本地副本。
 *   - 服务端 InstanceRegistry / RemoteRegionRegistry 查的是"权威内存注册表"，读的是"事实"。
 *   契约相同、数据源透明：任何组件注入哪种实现都用同一套调用方式，既解耦又好测试。
 *
 * 为什么带一个用不上的泛型 <T>？(原英文 Javadoc 只含糊写了 "for backward compatibility")
 *   T 在本接口的四个方法签名里从未出现，对客户端纯属摆设——客户端 EurekaClient 直接
 *   extends LookupService(裸类型，无类型参数)。T 是为服务端保留的：服务端要按 String(appName)
 *   维度查注册表，于是 InstanceRegistry extends LookupService<String>、
 *   RemoteRegionRegistry implements LookupService<String>。当年为让一个接口同时容纳
 *   "客户端 + 服务端"两种用法而引入 <T>，留到今天就是纯向后兼容，读源码时不必纠结它。
 */
public interface LookupService<T> {

    /**
     * Returns the corresponding {@link Application} object which is basically a
     * container of all registered <code>appName</code> {@link InstanceInfo}s.
     *
     * @param appName
     * @return a {@link Application} or null if we couldn't locate any app of
     *         the requested appName
     */
    // 【按应用名取一个 Application】
    // 客户端实现就是"读本地缓存再按名字捞"(见 DiscoveryClient#getApplication，最终落到
    // localRegionApps.get())。返回的是"我本地此刻认知的"该应用全部实例的容器，并非去服务端实时查；
    // 新鲜度取决于上一次注册表刷新(全量/增量)。查不到返回 null(注意不是空 Application)。
    Application getApplication(String appName);

    /**
     * Returns the {@link Applications} object which is basically a container of
     * all currently registered {@link Application}s.
     *
     * @return {@link Applications}
     */
    // 【取整张注册表快照】
    // 客户端实现直接返回本地缓存引用 localRegionApps.get()，不做拷贝——这是 Eureka 客户端
    // 服务发现的核心数据结构。之所以"直接返回引用还线程安全"，是因为它是 AtomicReference：
    // 刷新时用 set(...) 整体原子替换(copy-on-write 思路)，业务线程要么读到旧版要么读到新版，
    // 绝不会读到"改了一半"的中间态。坑：返回的是共享对象，调用方切勿原地修改它。
    Applications getApplications();

    /**
     * Returns the {@link List} of {@link InstanceInfo}s matching the the passed
     * in id. A single {@link InstanceInfo} can possibly be registered w/ more
     * than one {@link Application}s
     *
     * @param id
     * @return {@link List} of {@link InstanceInfo}s or
     *         {@link java.util.Collections#emptyList()}
     */
    // 【按实例 ID 全局扫描】
    // 实现是遍历本地缓存里"所有"Application 逐个查(见 DiscoveryClient#getInstancesById)。
    // 为什么要全表扫？因为同一个实例 ID 可能同时挂在多个应用名下(见上方英文 Javadoc)，
    // 所以返回 List——一个 ID 可能对应多个应用上下文，需调用方自行甄别。没命中返回空列表，不是 null。
    List<InstanceInfo> getInstancesById(String id);

    /**
     * Gets the next possible server to process the requests from the registry
     * information received from eureka.
     *
     * <p>
     * The next server is picked on a round-robin fashion. By default, this
     * method just returns the servers that are currently with
     * {@link com.netflix.appinfo.InstanceInfo.InstanceStatus#UP} status.
     * This configuration can be controlled by overriding the
     * {@link com.netflix.discovery.EurekaClientConfig#shouldFilterOnlyUpInstances()}.
     *
     * Note that in some cases (Eureka emergency mode situation), the instances
     * that are returned may not be unreachable, it is solely up to the client
     * at that point to timeout quickly and retry the next server.
     * </p>
     *
     * @param virtualHostname
     *            the virtual host name that is associated to the servers.
     * @param secure
     *            indicates whether this is a HTTP or a HTTPS request - secure
     *            means HTTPS.
     * @return the {@link InstanceInfo} information which contains the public
     *         host name of the next server in line to process the request based
     *         on the round-robin algorithm.
     * @throws java.lang.RuntimeException if the virtualHostname does not exist
     */
    // 【客户端软负载均衡入口：挑下一台服务器】
    // 四个方法里唯一"带决策"的——前三个只是查，这个还要替调用方选出一台。
    // 参数 virtualHostname 其实是 VIP(虚拟地址，对应 InstanceInfo.vipAddress)：一个应用的
    // 多个实例共享同一个 VIP，按 VIP 聚合成一组候选，这就是软负载均衡的分组依据。
    // 轮询如何做到线程安全：每个 VIP 各自持有一个 AtomicLong 计数器(Applications 内的
    // VipIndexSupport.roundRobinIndex)，每次 incrementAndGet() 自增后对候选数取模选中一台；
    // "每 VIP 一个计数器"而非全局计数器，是为了避免不同 VIP 互抢同一个热点。
    // 默认只返回 UP 实例、却几乎不耗 CPU：过滤不在这里实时做，而是在注册表"刷新阶段"一次性算好——
    // shuffleAndFilterInstances() 把非 UP 实例原地剔除并打乱后存进 vipList，是否过滤由
    // EurekaClientConfig#shouldFilterOnlyUpInstances() 控制(默认 true)，所以这里直接拿现成列表轮询。
    // 坑(务必结合上方英文 Javadoc 那句一起读)：原文 "may not be unreachable" 措辞反常，疑为笔误，
    // 本意是"返回的实例有可能是不可达的"。当实例全非 UP、过滤后列表为空时，紧急模式(emergency mode)
    // 下宁可把可疑实例也兜底发出来；能否连上由调用方负责：快速超时、立刻重试下一台。
    // 而本地这组 VIP 一个实例都没有时，则直接抛 RuntimeException。
    // 读 vs 写分工：此方法只对客户端有意义；服务端实现是空壳 return null
    // (PeerAwareInstanceRegistryImpl / RemoteRegionRegistry)——负载均衡的决策权属于客户端，
    // 服务端只管存所有实例的状态，不替客户端选机器。
    InstanceInfo getNextServerFromEureka(String virtualHostname, boolean secure);
}
