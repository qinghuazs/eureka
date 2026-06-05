package com.netflix.eureka.registry;

import com.netflix.appinfo.EurekaAccept;
import com.netflix.eureka.Version;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * 服务端 ResponseCache 三级缓存的【缓存键】（对应文档03"回源链入口"）。
 *
 * <p>整条读取链路是：客户端 GET 注册表 -> ResponseCache.get(Key) 命中缓存直接返回字节串，
 * 未命中则"回源"（读 readWriteCacheMap / 真正去 registry 序列化一份）。Key 就是这条链的入口标识，
 * 它决定了"哪一份序列化结果"被命中或被生成。</p>
 *
 * <p>关键设计：缓存键按"实体 + 序列化格式(JSON/XML) + 接口版本 + 地域(regions) + EurekaAccept(full/compact)"
 * 多个维度共同区分。也就是说【同一份注册表数据，会按不同格式 / 版本 / full|compact 各缓存一份独立的序列化结果】，
 * 这正是文档09所说"用内存换 CPU"的物理体现：宁可在内存里多放几份字节串，也不愿每次请求都重复做一次昂贵的序列化。</p>
 */
public class Key {

    // 序列化输出格式：JSON 或 XML。客户端通过 Accept 头选择，二者各自缓存一份。
    public enum KeyType {
        JSON, XML
    }

    /**
     * An enum to define the entity that is stored in this cache for this key.
     *
     * 缓存键所指向的"实体粒度"：
     * - Application：单个应用（按 appName 取），或全量注册表（约定的特殊名）。
     * - VIP：虚拟 IP 地址分组（vipAddress），按业务虚拟地址聚合的一组实例。
     * - SVIP：安全虚拟 IP（secureVipAddress），HTTPS 场景下的虚拟地址分组。
     * 不同 EntityType 即使 entityName 相同也是不同的键，互不污染缓存。
     */
    public enum EntityType {
        Application, VIP, SVIP
    }

    // 实体名：随 entityType 含义不同——可能是 appName、vipAddress 或 secureVipAddress；全量注册表用约定的特殊名。
    private final String entityName;
    // 地域列表：跨 region 联邦拉取时携带的远程区域名。为空表示只取本地 region；不同 regions 视为不同缓存键。
    private final String[] regions;
    // 序列化格式（JSON / XML），见 KeyType。
    private final KeyType requestType;
    // 接口版本（V1 / V2 等），不同版本的 DTO 结构不同，必须各缓存一份。
    private final Version requestVersion;
    // 由下方六要素拼接而成的最终键字符串，equals / hashCode 全部基于它——这是缓存命中与否的真正判据。
    private final String hashKey;
    // 实体粒度（Application / VIP / SVIP），见 EntityType。
    private final EntityType entityType;
    // 客户端期望的内容粒度：full（完整实例信息）或 compact（精简）。full 与 compact 各缓存一份独立结果。
    private final EurekaAccept eurekaAccept;

    // 便捷构造器：不带 regions（即只查本地 region），转调下面的全参构造器并把 regions 置空。
    public Key(EntityType entityType, String entityName, KeyType type, Version v, EurekaAccept eurekaAccept) {
        this(entityType, entityName, type, v, eurekaAccept, null);
    }

    public Key(EntityType entityType, String entityName, KeyType type, Version v, EurekaAccept eurekaAccept, @Nullable String[] regions) {
        this.regions = regions;
        this.entityType = entityType;
        this.entityName = entityName;
        this.requestType = type;
        this.requestVersion = v;
        this.eurekaAccept = eurekaAccept;
        // 【核心】六要素拼接成唯一缓存键：entityType + entityName + regions(地域) + requestType(格式) + requestVersion(版本) + eurekaAccept(full/compact)。
        // 任一维度不同即生成不同的 hashKey，从而在 ResponseCache 中命中/落到不同的缓存槽位——
        // 这就是"同一份注册表数据按格式/版本/地域/full|compact 各缓存一份序列化结果"的来源（用内存换 CPU，见文档09）。
        // 注意：仅 regions 非空时才把数组转成字符串参与拼接，避免空数组与 null 产生不同的键。
        hashKey = this.entityType + this.entityName + (null != this.regions ? Arrays.toString(this.regions) : "")
                + requestType.name() + requestVersion.name() + this.eurekaAccept.name();
    }

    public String getName() {
        return entityName;
    }

    // 返回六要素拼出的最终键字符串，是 equals/hashCode 的唯一依据，也是 ResponseCache 内部用来做缓存查找的标识。
    public String getHashKey() {
        return hashKey;
    }

    public KeyType getType() {
        return requestType;
    }

    public Version getVersion() {
        return requestVersion;
    }

    public EurekaAccept getEurekaAccept() {
        return eurekaAccept;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public boolean hasRegions() {
        return null != regions && regions.length != 0;
    }

    public String[] getRegions() {
        return regions;
    }

    // 克隆出一个"去掉 regions"的等价键。用于回源逻辑：远程 region 的数据需先用本地键（无 regions）取本地结果，
    // 再叠加远程内容，因此要从带 regions 的请求键派生出对应的本地键。
    public Key cloneWithoutRegions() {
        return new Key(entityType, entityName, requestType, requestVersion, eurekaAccept);
    }

    // hashCode 直接委托给 hashKey 字符串——保证"六要素相同的两个 Key 落在同一哈希桶"，
    // 使其能作为 ConcurrentMap / Guava Cache 的键正常工作。
    @Override
    public int hashCode() {
        String hashKey = getHashKey();
        return hashKey.hashCode();
    }

    // 相等性同样只看 hashKey：只要六要素拼出的字符串一致就视为同一个缓存键。
    // 这是缓存能正确命中的语义前提——两次请求只要实体/格式/版本/地域/full|compact 全部相同，就复用同一份序列化结果。
    @Override
    public boolean equals(Object other) {
        if (other instanceof Key) {
            return getHashKey().equals(((Key) other).getHashKey());
        } else {
            return false;
        }
    }

    // 精简的可读表示，仅用于日志/调试输出（不参与相等判断），便于排查"某个请求命中了哪类缓存键"。
    public String toStringCompact() {
        StringBuilder sb = new StringBuilder();
        sb.append("{name=").append(entityName).append(", type=").append(entityType).append(", format=").append(requestType);
        if(regions != null) {
            sb.append(", regions=").append(Arrays.toString(regions));
        }
        sb.append('}');
        return sb.toString();
    }
}
