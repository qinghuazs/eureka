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

import javax.annotation.Nullable;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.netflix.discovery.util.MapUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.InstanceRegionChecker;
import com.netflix.discovery.provider.Serializer;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * The class that wraps all the registry information returned by eureka server.
 *
 * <p>
 * Note that the registry information is fetched from eureka server as specified
 * in {@link EurekaClientConfig#getRegistryFetchIntervalSeconds()}. Once the
 * information is fetched it is shuffled and also filtered for instances with
 * {@link InstanceStatus#UP} status as specified by the configuration
 * {@link EurekaClientConfig#shouldFilterOnlyUpInstances()}.
 * </p>
 *
 * @author Karthik Ranganathan
 *
 */
/*
 * 【中文说明 · 对应文档03/09:增量拉取与一致性指纹】
 * Applications 是客户端本地"全量注册表快照"的载体：它把服务端返回的所有 Application
 * (每个应用下若干 InstanceInfo) 聚合在一起，并维护 VIP/SecureVIP 的路由索引。
 *
 * 本类在整条拉取流程里最关键的角色,是承载"一致性指纹 (reconcile hash code)"这套
 * 增量拉取的兜底校验机制:
 *  - 客户端首次走全量拉取拿到完整注册表,之后每个周期只拉【增量 delta】(新增/修改/删除若干实例)。
 *  - 客户端把 delta 合并进本地这份 Applications 快照后,本地用 getReconcileHashCode() 自行
 *    算出一个指纹字符串(形如 "UP_10_DOWN_2_"),再和服务端在响应里带回来的 appsHashCode 比对。
 *  - 若两者相等 => 认为本地快照与服务端一致,本轮增量合并成功,无需做任何额外动作;
 *    若不相等 => 说明增量合并过程中丢了/串了数据,客户端立即放弃增量、改走一次【全量拉取】做兜底
 *    (即文档09里说的 reconcile/对账)。
 *
 * 注意这只是"低成本一致性自检",不是强一致保证:见下方 getReconcileHashCode 的说明,
 * 指纹只编码"各状态有多少个实例",对实例的具体内容(IP/端口/元数据/到底是哪台上线哪台下线)不敏感。
 */
@Serializer("com.netflix.discovery.converters.EntityBodyConverter")
@XStreamAlias("applications")
@JsonRootName("applications")
public class Applications {
    private static class VipIndexSupport {
        // Progressive list: emptyList (0) -> singletonList (1) -> ArrayList (2+)
        // This avoids CLQ and Node allocations. 56% of VIPs have exactly 1 instance.
        private List<InstanceInfo> instances = Collections.emptyList();
        final AtomicLong roundRobinIndex = new AtomicLong(0);
        private volatile List<InstanceInfo> vipList = Collections.emptyList();

        void addInstance(InstanceInfo info) {
            int size = instances.size();
            if (size == 0) {
                // 0 -> 1: use singletonList (56% of VIPs stop here)
                instances = Collections.singletonList(info);
            } else if (size == 1) {
                // 1 -> 2: transition singletonList to ArrayList.
                // Capacity 12 chosen based on prod data analysis: covers 81% of multi-instance
                // VIPs without resize (spikes at 6, 9, 12 instances from 3-AZ deployments).
                // ArrayList grows 1.5x (12->18->27), aligning well with common sizes.
                // Capacity 12 minimizes total allocation vs smaller capacities.
                InstanceInfo first = instances.get(0);
                ArrayList<InstanceInfo> list = new ArrayList<>(12);
                list.add(first);
                list.add(info);
                instances = list;
            } else {
                // 2+ -> n: append to ArrayList
                ((ArrayList<InstanceInfo>) instances).add(info);
            }
        }

        int instanceCount() {
            return instances.size();
        }

        List<InstanceInfo> getInstances() {
            return instances;
        }

        public AtomicLong getRoundRobinIndex() {
            return roundRobinIndex;
        }

        List<InstanceInfo> getVipList() {
            return vipList;
        }

        void setVipList(List<InstanceInfo> vipList) {
            this.vipList = vipList;
        }
    }

    // 指纹字符串里"状态名"与"数量"之间、以及各段之间的分隔符,例如 UP_10_DOWN_2_ 里的下划线
    private static final String STATUS_DELIMITER = "_";

    // 【一致性指纹 · 文档09核心字段】服务端在拉取响应里带回的注册表指纹。
    // 客户端把本轮增量(delta)合并到本地快照后,会用 getReconcileHashCode() 自算一个指纹,
    // 与这个 appsHashCode 比对:相等=>增量合并一致;不相等=>触发一次全量拉取兜底(reconcile)。
    // 它本身只是个普通字符串(形如 "UP_10_DOWN_2_"),并不参与 Java 对象的 equals/hashCode。
    private String appsHashCode;
    // 增量版本号(delta 版本),由服务端维护并随响应返回,用于标识"这是第几代增量",辅助判断 delta 是否衔接。
    private Long versionDelta;
    @XStreamImplicit
    // 本地持有的全部应用集合;用 ConcurrentLinkedQueue 是为了在拉取线程更新、调用线程读取时弱一致并发安全
    private final AbstractQueue<Application> applications;
    // 应用名(大写) -> Application 的索引,供按 appName 快速定位
    private final Map<String, Application> appNameApplicationMap;
    // VIP 地址(大写) -> 该 VIP 下实例的路由索引(含轮询游标 + 洗牌后的实例列表),供按 VIP 做客户端负载均衡
    private final Map<String, VipIndexSupport> virtualHostNameAppMap;
    // 安全 VIP(HTTPS)地址 -> 路由索引,语义同上,只是针对 secureVipAddress
    private final Map<String, VipIndexSupport> secureVirtualHostNameAppMap;

    /**
     * Create a new, empty Eureka application list.
     */
    public Applications() {
        this(null, -1L, Collections.emptyList());
    }

    /**
     * Note that appsHashCode and versionDelta key names are formatted in a
     * custom/configurable way.
     */
    @JsonCreator
    public Applications(@JsonProperty("appsHashCode") String appsHashCode,
            @JsonProperty("versionDelta") Long versionDelta,
            @JsonProperty("application") List<Application> registeredApplications) {
        this.applications = new ConcurrentLinkedQueue<Application>();
        this.appNameApplicationMap = new ConcurrentHashMap<String, Application>();
        this.virtualHostNameAppMap = new ConcurrentHashMap<String, VipIndexSupport>();
        this.secureVirtualHostNameAppMap = new ConcurrentHashMap<String, VipIndexSupport>();
        this.appsHashCode = appsHashCode;
        this.versionDelta = versionDelta;

        for (Application app : registeredApplications) {
            this.addApplication(app);
        }
    }

    /**
     * Add the <em>application</em> to the list.
     *
     * @param app
     *            the <em>application</em> to be added.
     */
    public void addApplication(Application app) {
        appNameApplicationMap.put(app.getName().toUpperCase(Locale.ROOT), app);
        addInstancesToVIPMaps(app, this.virtualHostNameAppMap, this.secureVirtualHostNameAppMap);
        applications.add(app);
    }

    /**
     * Gets the list of all registered <em>applications</em> from eureka.
     *
     * @return list containing all applications registered with eureka.
     */
    @JsonProperty("application")
    public List<Application> getRegisteredApplications() {
        return new ArrayList<Application>(this.applications);
    }

    /**
     * Returns whether there are any registered applications.
     * This is more efficient than {@code getRegisteredApplications().isEmpty()}
     * as it avoids creating a defensive copy.
     *
     * @return true if there are no registered applications
     */
    public boolean isRegisteredApplicationsEmpty() {
        return this.applications.isEmpty();
    }

    /**
     * Gets the registered <em>application</em> for the given
     * application name.
     *
     * @param appName
     *            the application name for which the result need to be fetched.
     * @return the registered application for the given application
     *         name.
     */
    public Application getRegisteredApplications(String appName) {
        return appNameApplicationMap.get(appName.toUpperCase(Locale.ROOT));
    }

    /**
     * Gets the list of <em>instances</em> associated to a virtual host name.
     *
     * @param virtualHostName
     *            the virtual hostname for which the instances need to be
     *            returned.
     * @return list of <em>instances</em>.
     */
    public List<InstanceInfo> getInstancesByVirtualHostName(String virtualHostName) {
        return Optional.ofNullable(this.virtualHostNameAppMap.get(virtualHostName.toUpperCase(Locale.ROOT)))
            .map(VipIndexSupport::getVipList)
            .orElseGet(Collections::emptyList);
    }

    /**
     * Gets the list of secure <em>instances</em> associated to a virtual host
     * name.
     *
     * @param secureVirtualHostName
     *            the virtual hostname for which the secure instances need to be
     *            returned.
     * @return list of <em>instances</em>.
     */
    public List<InstanceInfo> getInstancesBySecureVirtualHostName(String secureVirtualHostName) {
        return Optional.ofNullable(this.secureVirtualHostNameAppMap.get(secureVirtualHostName.toUpperCase(Locale.ROOT)))
                .map(VipIndexSupport::getVipList)
                .orElseGet(Collections::emptyList);
    }

    /**
     * @return a weakly consistent size of the number of instances in all the
     *         applications
     */
    public int size() {
        return applications.stream().mapToInt(Application::size).sum();
    }

    @Deprecated
    public void setVersion(Long version) {
        this.versionDelta = version;
    }

    @Deprecated
    @JsonIgnore // Handled directly due to legacy name formatting
    public Long getVersion() {
        return this.versionDelta;
    }

    /**
     * Used by the eureka server. Not for external use.
     *
     * @param hashCode
     *            the hash code to assign for this app collection
     */
    public void setAppsHashCode(String hashCode) {
        this.appsHashCode = hashCode;
    }

    /**
     * Used by the eureka server. Not for external use.
     * 
     * @return the string indicating the hashcode based on the applications
     *         stored.
     *
     */
    @JsonIgnore // Handled directly due to legacy name formatting
    public String getAppsHashCode() {
        return this.appsHashCode;
    }

    /**
     * Gets the hash code for this <em>applications</em> instance. Used for
     * comparison of instances between eureka server and eureka client.
     *
     * @return the internal hash code representation indicating the information
     *         about the instances.
     */
    /*
     * 【一致性指纹入口 · 文档09核心方法】对"当前这份本地快照"算出指纹字符串。
     * 客户端在合并完增量(delta)后调用本方法,把结果与服务端返回的 appsHashCode 比对,
     * 决定要不要走全量拉取兜底。
     *
     * 实现分两步:
     *  1) populateInstanceCountMap:遍历所有实例,按 InstanceStatus 统计【各状态各有多少个实例】;
     *     这里用 TreeMap 是关键——按状态名升序排列,保证客户端和服务端拼出来的字符串顺序一致,
     *     否则同样的计数因 Map 遍历顺序不同会得到不同字符串,导致误判。
     *  2) getReconcileHashCode(map):把计数 Map 拼成 "状态_数量_状态_数量_" 形式的指纹。
     *
     * 划重点(文档03/09反复强调):指纹只编码"各状态实例的数量",
     * 对实例的具体内容(IP、端口、元数据、到底是哪台机器上线/下线)完全不敏感。
     * 因此只能粗粒度地发现"数量对不上"这类增量丢失,不能保证逐实例内容一致。
     */
    @JsonIgnore
    public String getReconcileHashCode() {
        TreeMap<String, AtomicInteger> instanceCountMap = new TreeMap<String, AtomicInteger>();
        populateInstanceCountMap(instanceCountMap);
        return getReconcileHashCode(instanceCountMap);
    }

    /**
     * Populates the provided instance count map. The instance count map is used
     * as part of the general app list synchronization mechanism.
     * 
     * @param instanceCountMap
     *            the map to populate
     */
    /*
     * 【指纹的数据来源 · 统计各状态实例数量】把"当前快照里每种 InstanceStatus 各有多少个实例"
     * 填进 instanceCountMap(状态名 -> 计数)。这一步就是指纹"只看数量、不看内容"的根因所在:
     * 它把每个实例坍缩成"它处于哪个状态",其余信息全部丢弃。
     *
     * 性能上做了两段式优化(因为该方法在每个拉取周期都会跑,且实例可能上万):
     *  1) 先用一个按 ordinal 下标的 int[] 计数,遍历所有实例时只做一次自增,
     *     避免在热路径上反复操作 Map / 装箱;
     *  2) 再单趟把 int[] 里非零的状态写进对外的 Map(只暴露真正出现过的状态)。
     */
    public void populateInstanceCountMap(Map<String, AtomicInteger> instanceCountMap) {
        // accrue here as lightweight as possible
        // 用状态枚举的 ordinal 作为下标的轻量计数数组,遍历期间零额外分配
        int[] statusCounts = new int[InstanceStatus.values().length];
        // 对每个实例:取其状态,在对应下标上 +1
        Consumer<InstanceInfo> countByStatus = info -> statusCounts[info.getStatus().ordinal()]++;
        for (Application app : this.applications) {
            app.forEachInstance(countByStatus);
        }

        // now convert it over to the API form in a single pass
        // 把数组里的计数单趟转成对外 Map;count>0 的过滤保证"没有该状态的实例就不写入",
        // 这样后续指纹串里也不会出现 0 计数的噪声段
        for (InstanceStatus status : InstanceStatus.values()) {
            int count = statusCounts[status.ordinal()];
            if (count > 0) {
                instanceCountMap.computeIfAbsent(status.name(), k -> new AtomicInteger(0))
                    .addAndGet(count);
            }
        }
    }

    /**
     * Gets the reconciliation hashcode. The hashcode is used to determine
     * whether the applications list has changed since the last time it was
     * acquired.
     * 
     * @param instanceCountMap
     *            the instance count map to use for generating the hash
     * @return the hash code for this instance
     */
    /*
     * 【指纹拼装 · 把计数 Map 拼成 "状态_数量_状态_数量_"】例如有 10 个 UP、2 个 DOWN 的实例,
     * 传入的(已按状态名排序的)Map 会被拼成字符串 "DOWN_2_UP_10_"。
     * 每段格式为:状态名 + "_" + 数量 + "_"。
     *
     * 之所以能拿来做客户端/服务端比对:只要两边"各状态的实例总数"完全相同,且遍历顺序一致
     * (调用方用 TreeMap 保证有序),拼出来的字符串就逐字符相等。客户端正是用这个字符串去和
     * 服务端响应里的 appsHashCode 做 String 相等判断,决定本轮增量是否可信、要不要全量兜底。
     * 再次强调:它对实例的具体身份/内容无感知,只是个廉价的"数量级一致性"校验。
     */
    public static String getReconcileHashCode(Map<String, AtomicInteger> instanceCountMap) {
        StringBuilder reconcileHashCode = new StringBuilder(75);
        for (Map.Entry<String, AtomicInteger> mapEntry : instanceCountMap.entrySet()) {
            reconcileHashCode.append(mapEntry.getKey()).append(STATUS_DELIMITER).append(mapEntry.getValue().get())
                    .append(STATUS_DELIMITER);
        }
        return reconcileHashCode.toString();
    }

    /**
     * Shuffles the provided instances so that they will not always be returned
     * in the same order.
     * 
     * @param filterUpInstances
     *            whether to return only UP instances
     */
    public void shuffleInstances(boolean filterUpInstances) {
        shuffleInstances(filterUpInstances, false, null, null, null);
    }

    /**
     * Shuffles a whole region so that the instances will not always be returned
     * in the same order.
     * 
     * @param remoteRegionsRegistry
     *            the map of remote region names to their registries
     * @param clientConfig
     *            the {@link EurekaClientConfig}, whose settings will be used to
     *            determine whether to filter to only UP instances
     * @param instanceRegionChecker
     *            the instance region checker
     */
    public void shuffleAndIndexInstances(Map<String, Applications> remoteRegionsRegistry,
            EurekaClientConfig clientConfig, InstanceRegionChecker instanceRegionChecker) {
        shuffleInstances(clientConfig.shouldFilterOnlyUpInstances(), true, remoteRegionsRegistry, clientConfig,
                instanceRegionChecker);
    }

    private void shuffleInstances(boolean filterUpInstances, 
            boolean indexByRemoteRegions,
            @Nullable Map<String, Applications> remoteRegionsRegistry, 
            @Nullable EurekaClientConfig clientConfig,
            @Nullable InstanceRegionChecker instanceRegionChecker) {
        Map<String, VipIndexSupport> secureVirtualHostNameAppMap = MapUtil.newHashMapWithExpectedSize(this.secureVirtualHostNameAppMap.size());
        Map<String, VipIndexSupport> virtualHostNameAppMap = MapUtil.newHashMapWithExpectedSize(this.virtualHostNameAppMap.size());
        for (Application application : appNameApplicationMap.values()) {
            if (indexByRemoteRegions) {
                application.shuffleAndStoreInstances(remoteRegionsRegistry, clientConfig, instanceRegionChecker);
            } else {
                application.shuffleAndStoreInstances(filterUpInstances);
            }
            this.addInstancesToVIPMaps(application, virtualHostNameAppMap, secureVirtualHostNameAppMap);
        }
        shuffleAndFilterInstances(virtualHostNameAppMap, filterUpInstances);
        shuffleAndFilterInstances(secureVirtualHostNameAppMap, filterUpInstances);

        this.virtualHostNameAppMap.putAll(virtualHostNameAppMap);
        this.virtualHostNameAppMap.keySet().retainAll(virtualHostNameAppMap.keySet());
        this.secureVirtualHostNameAppMap.putAll(secureVirtualHostNameAppMap);
        this.secureVirtualHostNameAppMap.keySet().retainAll(secureVirtualHostNameAppMap.keySet());
    }

    /**
     * Gets the next round-robin index for the given virtual host name. This
     * index is reset after every registry fetch cycle.
     *
     * @param virtualHostname
     *            the virtual host name.
     * @param secure
     *            indicates whether it is a secure request or a non-secure
     *            request.
     * @return AtomicLong value representing the next round-robin index.
     */
    public AtomicLong getNextIndex(String virtualHostname, boolean secure) {
        Map<String, VipIndexSupport> index = (secure) ? secureVirtualHostNameAppMap : virtualHostNameAppMap;
        return Optional.ofNullable(index.get(virtualHostname.toUpperCase(Locale.ROOT)))
                .map(VipIndexSupport::getRoundRobinIndex)
                .orElse(null);
    }

    /**
     * Shuffle the instances and filter for only {@link InstanceStatus#UP} if
     * required.
     *
     */
    private void shuffleAndFilterInstances(Map<String, VipIndexSupport> srcMap, boolean filterUpInstances) {
        Random shuffleRandom = new Random();
        for (Map.Entry<String, VipIndexSupport> entries : srcMap.entrySet()) {
            shuffleAndFilterInstances(entries.getValue(), filterUpInstances, shuffleRandom);
        }
    }

    /**
     * Shuffle and filter instances for a single VIP.
     */
    private void shuffleAndFilterInstances(VipIndexSupport vipIndexSupport, boolean filterUpInstances, Random shuffleRandom) {
        List<InstanceInfo> instances = vipIndexSupport.getInstances();
        int size = instances.size();

        // Empty: nothing to do
        if (size == 0) {
            vipIndexSupport.setVipList(instances);
            return;
        }

        // Single instance: no shuffle needed, check status if filtering
        if (size == 1) {
            InstanceInfo instance = instances.get(0);
            boolean keep = !filterUpInstances || instance.getStatus() == InstanceStatus.UP;
            vipIndexSupport.setVipList(keep ? instances : Collections.emptyList());
            return;
        }

        // Multiple instances (2+): instances is always an ArrayList at this point
        ArrayList<InstanceInfo> list = (ArrayList<InstanceInfo>) instances;

        // Filter in place if needed (no-op when all instances are UP)
        if (filterUpInstances) {
            filterToUpInstancesInPlace(list);
            if (list.isEmpty()) {
                vipIndexSupport.setVipList(Collections.emptyList());
                return;
            }
        }

        // Shuffle in place and reuse
        Collections.shuffle(list, shuffleRandom);
        vipIndexSupport.setVipList(list);
    }

    /**
     * Filter list in place to keep only UP instances. Allocation-free.
     */
    private static void filterToUpInstancesInPlace(ArrayList<InstanceInfo> list) {
        int size = list.size();
        int writeIndex = 0;
        // shift forward all of the UP instances
        for (int i = 0; i < size; i++) {
            InstanceInfo instance = list.get(i);
            if (instance.getStatus() == InstanceStatus.UP) {
                if (writeIndex != i) {
                    list.set(writeIndex, instance);
                }
                writeIndex++;
            }
        }
        // Truncate: remove tail elements. Allows old objects to be GCd.
        // Array is not shrunk back, but, in the majority case this is not useful.
        // More important that we clear the entries so the InstanceInfo elements
        // can be released.
        if (writeIndex < size) {
            list.subList(writeIndex, size).clear();
        }
    }

    /**
     * Add the instance to the given map based if the vip address matches with
     * that of the instance. Note that an instance can be mapped to multiple vip
     * addresses.
     */
    private void addInstanceToMap(InstanceInfo info, String vipAddresses, Map<String, VipIndexSupport> vipMap) {
        // This code path is quite hot on allocations. We apply common-case optimizations to minimize allocations.
        // Gathered statistics from a real cluster:
        // | Metric                | Test   | Prod    |
        // |-----------------------|--------|---------|
        // | Total entries         | N      | 2x N    |
        // | Single VIP (no comma) | 91.1%  | 91.7%   |
        // | 2 VIPs                | 6.9%   | 5.9%    |
        // | 3+ VIPs               | 0.4%   | 0.7%    |
        // | Empty                 | 1.6%   | 1.7%    |
        // | Max VIPs per entry    | 7      | 13      |
        // | Avg string length     | 29.5   | 25.8    |
        // | Max string length     | 204    | 468     |

        // Note: empty vipAddresses is intentionally allowed for backwards compatibility.
        // Legacy behavior: "" creates a mapping with empty string key.
        if (vipAddresses == null) {
            return;
        }

        String upper = vipAddresses.toUpperCase(Locale.ROOT);

        // Fast path (91.7% of cases) single VIP: no split() -> byte[], no substring() -> String
        int commaIndex = upper.indexOf(',');
        if (commaIndex == -1) {
            vipMap.computeIfAbsent(upper, k -> new VipIndexSupport()).addInstance(info);
            return;
        }

        // Multiple VIPs: uppercase once, then parse without split() byte[] allocation
        int start = 0;
        do {
            String vipAddress = upper.substring(start, commaIndex);
            vipMap.computeIfAbsent(vipAddress, k -> new VipIndexSupport()).addInstance(info);
            start = commaIndex + 1;
        } while ((commaIndex = upper.indexOf(',', start)) != -1);

        // Last segment
        String vipAddress = upper.substring(start);
        vipMap.computeIfAbsent(vipAddress, k -> new VipIndexSupport()).addInstance(info);
    }

    /**
     * Adds the instances to the internal vip address map.
     * 
     * @param app
     *            - the applications for which the instances need to be added.
     */
    private void addInstancesToVIPMaps(Application app, Map<String, VipIndexSupport> virtualHostNameAppMap,
            Map<String, VipIndexSupport> secureVirtualHostNameAppMap) {
        // Check and add the instances to the their respective virtual host name
        // mappings
        for (InstanceInfo info : app.getInstances()) {
            String vipAddresses = info.getVIPAddress();
            if (vipAddresses != null) {
                addInstanceToMap(info, vipAddresses, virtualHostNameAppMap);
            }

            String secureVipAddresses = info.getSecureVipAddress();
            if (secureVipAddresses != null) {
                addInstanceToMap(info, secureVipAddresses, secureVirtualHostNameAppMap);
            }
        }
    }

    /**
     * Remove the <em>application</em> from the list.
     *
     * @param app the <em>application</em>
     */
    public void removeApplication(Application app) {
        this.appNameApplicationMap.remove(app.getName().toUpperCase(Locale.ROOT));
        this.applications.remove(app);
    }
}
