package com.netflix.eureka.registry;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.discovery.shared.LookupService;
import com.netflix.discovery.shared.Pair;
import com.netflix.eureka.lease.LeaseManager;

import java.util.List;
import java.util.Map;

/**
 * @author Tomasz Bak
 */
/*
 * 【中文导读 · 原生 Netflix Eureka 的注册表契约接口】
 * 这是 eureka-core 中"实例注册表"的顶层抽象接口，定义了注册表对外暴露的全部契约。
 *
 * 继承关系(它把两个能力合二为一)：
 *   · extends LeaseManager<InstanceInfo> —— 租约的写侧：register / cancel / renew / evict
 *     (注册、注销、续约、过期剔除)，对应租约生命周期(文档09/租约相关)。
 *   · extends LookupService<String>     —— 读侧:按 appName/instanceId 查询应用与实例(对外提供发现能力)。
 * 因此本接口 = 写(租约管理) + 读(服务发现) + 服务端特有的运维/状态治理方法。
 *
 * 【特别提醒 · 不要与 SpringCloud 的同名类混淆】
 * 本仓库是原生 Netflix Eureka 1.x。这里的 InstanceRegistry 是「原生抽象契约接口」，
 * 其主要实现是 AbstractInstanceRegistry → PeerAwareInstanceRegistryImpl(本仓库内)。
 * SpringCloud Netflix 里也有一个叫 InstanceRegistry 的类，但那是「另一个项目」中
 * 继承 PeerAwareInstanceRegistryImpl 的子类(用于发布 Spring 事件等)，与本接口不是同一个东西。
 */
public interface InstanceRegistry extends LeaseManager<InstanceInfo>, LookupService<String> {

    // 开闸放行：初始化完成后让注册表对外开始接收流量，并据 count 预估每分钟续约数、设定自我保护阈值。
    void openForTraffic(ApplicationInfoManager applicationInfoManager, int count);

    void shutdown();

    /*
     * 存储/清理"覆盖状态"——对应文档07(状态覆盖责任链)的写入侧。
     * 把某实例的人工覆盖状态(overriddenStatus)记入注册表的覆盖状态表，后续 getOverriddenInstanceStatus
     * 走责任链判定时，OverrideExistsRule 会读取此处存的值。下面这个单参重载已废弃(缺 appName，无法精确定位)。
     */
    @Deprecated
    void storeOverriddenStatusIfRequired(String id, InstanceStatus overriddenStatus);

    // 同上，但带 appName 精确定位实例，是推荐使用的版本。
    void storeOverriddenStatusIfRequired(String appName, String id, InstanceStatus overriddenStatus);

    /*
     * 状态变更入口 —— 对应文档07(状态覆盖)的"手动改状态"路径。
     * 由 REST 层(StatusResource)PUT 状态时调用：把 newStatus 记为该实例的 overridden 状态并触发续约时间刷新，
     * 之后该状态会参与责任链判定。isReplication 标记本次是否来自 peer 节点的复制写入(影响是否再次向其它节点扩散)。
     */
    boolean statusUpdate(String appName, String id, InstanceStatus newStatus,
                         String lastDirtyTimestamp, boolean isReplication);

    /*
     * 删除"覆盖状态" —— 对应文档07，statusUpdate 的逆操作。
     * 由 REST 层 DELETE 状态时调用：清除该实例之前人工设置的 overridden 状态，让其状态回归由责任链按默认规则推导。
     */
    boolean deleteStatusOverride(String appName, String id, InstanceStatus newStatus,
                                 String lastDirtyTimestamp, boolean isReplication);

    Map<String, InstanceStatus> overriddenInstanceStatusesSnapshot();

    Applications getApplicationsFromLocalRegionOnly();

    List<Application> getSortedApplications();

    /**
     * Get application information.
     *
     * @param appName The name of the application
     * @param includeRemoteRegion true, if we need to include applications from remote regions
     *                            as indicated by the region {@link java.net.URL} by this property
     *                            {@link com.netflix.eureka.EurekaServerConfig#getRemoteRegionUrls()}, false otherwise
     * @return the application
     */
    Application getApplication(String appName, boolean includeRemoteRegion);

    /**
     * Gets the {@link InstanceInfo} information.
     *
     * @param appName the application name for which the information is requested.
     * @param id the unique identifier of the instance.
     * @return the information about the instance.
     */
    InstanceInfo getInstanceByAppAndId(String appName, String id);

    /**
     * Gets the {@link InstanceInfo} information.
     *
     * @param appName the application name for which the information is requested.
     * @param id the unique identifier of the instance.
     * @param includeRemoteRegions true, if we need to include applications from remote regions
     *                             as indicated by the region {@link java.net.URL} by this property
     *                             {@link com.netflix.eureka.EurekaServerConfig#getRemoteRegionUrls()}, false otherwise
     * @return the information about the instance.
     */
    InstanceInfo getInstanceByAppAndId(String appName, String id, boolean includeRemoteRegions);

    void clearRegistry();

    void initializedResponseCache();

    ResponseCache getResponseCache();

    long getNumOfRenewsInLastMin();

    int getNumOfRenewsPerMinThreshold();

    int isBelowRenewThresold();

    List<Pair<Long, String>> getLastNRegisteredInstances();

    List<Pair<Long, String>> getLastNCanceledInstances();

    /**
     * Checks whether lease expiration is enabled.
     * @return true if enabled
     */
    boolean isLeaseExpirationEnabled();

    boolean isSelfPreservationModeEnabled();

}
