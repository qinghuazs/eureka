package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;
import com.netflix.eureka.registry.AbstractInstanceRegistry;

/**
 * A single rule that if matched it returns an instance status.
 * The idea is to use an ordered list of such rules and pick the first result that matches.
 *
 * It is designed to be used by
 * {@link AbstractInstanceRegistry#getOverriddenInstanceStatus(InstanceInfo, Lease, boolean)}
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
/*
 * 【中文导读 · 对应文档07(状态覆盖责任链)】
 * 这是"状态覆盖(status override)责任链"中各节点的【统一接口】，是整条链的契约抽象。
 *
 * 背景：实例对外呈现的状态(UP/DOWN/OUT_OF_SERVICE/STARTING 等)不一定等于客户端自己上报的状态，
 * 服务端会按一组"覆盖规则"逐一判定，最终决定该实例真正生效(overridden)的状态。
 *
 * 设计模式：责任链 + 短路命中。服务端把若干实现按固定优先级组成一个【有序列表】，逐个调用 apply()：
 *   · 某个规则若"命中"，返回的 StatusOverrideResult.matches()==true，链立即短路，采用该规则给出的状态；
 *   · 若"未命中"(matches()==false / StatusOverrideResult.NO_MATCH)，则继续询问下一个规则。
 * 调用方为 {@link AbstractInstanceRegistry#getOverriddenInstanceStatus}，
 * 实际的链顺序在 AbstractInstanceRegistry 中以 FirstMatchWinsCompositeRule 组合而成
 * (典型顺序：显式覆盖 DownOrStartingRule → OverrideExistsRule → LeaseExistsRule 等，各实现已分别注释)。
 *
 * 本文件只定义"链上每个节点长什么样"，各具体规则的判定逻辑见同包下的各 *Rule 实现类。
 */
public interface InstanceStatusOverrideRule {

    /**
     * Match this rule.
     *
     * @param instanceInfo The instance info whose status we care about.
     * @param existingLease Does the instance have an existing lease already? If so let's consider that.
     * @param isReplication When overriding consider if we are under a replication mode from other servers.
     * @return A result with whether we matched and what we propose the status to be overriden to.
     */
    /*
     * 责任链节点的唯一方法：对单条实例运行本规则。
     * 返回 StatusOverrideResult 既表达"是否命中(matches)"、也携带"若命中应覆盖成什么状态"。
     * 命中即短路；调用方据此决定是采纳本结果还是把判定权交给链上的下一个规则。
     *
     * @param instanceInfo  要判定状态的实例信息(客户端上报视角)。
     * @param existingLease 服务端是否已存在该实例的租约；存在时其中保留的 overridden 状态/历史会被某些规则纳入考量。
     * @param isReplication 当前是否处于来自其它 peer 节点的复制(replication)写入；复制场景下部分规则的行为会不同，
     *                      以避免节点间相互覆盖产生抖动。
     * @return 判定结果：是否命中 + 建议覆盖成的目标状态。
     */
    StatusOverrideResult apply(final InstanceInfo instanceInfo,
                               final Lease<InstanceInfo> existingLease,
                               boolean isReplication);

}
