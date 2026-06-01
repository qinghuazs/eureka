package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This rule matches if the instance is DOWN or STARTING.
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
public class DownOrStartingRule implements InstanceStatusOverrideRule {
    private static final Logger logger = LoggerFactory.getLogger(DownOrStartingRule.class);

    // 【规则①：故障状态优先】实例自报"我有问题"（DOWN/STARTING/UNKNOWN）时无条件相信。
    // 设计逻辑（非对称信任）：
    //   - 说自己"坏"→ 直接信：实例自己最清楚自己起没起来、健康检查过没过，谎报"坏"没有动机
    //   - 说自己"好"(UP) → 存疑，交给后面的规则验证：因为可能存在运维的覆盖状态(OUT_OF_SERVICE)
    //     把它压制着，不能让实例一上报 UP 就把运维的设置冲掉
    //   - OUT_OF_SERVICE 同样存疑：需要后续规则确认（可能已被运维删除覆盖）
    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease,
                                      boolean isReplication) {
        // ReplicationInstance is DOWN or STARTING - believe that, but when the instance says UP, question that
        // The client instance sends STARTING or DOWN (because of heartbeat failures), then we accept what
        // the client says. The same is the case with replica as well.
        // The OUT_OF_SERVICE from the client or replica needs to be confirmed as well since the service may be
        // currently in SERVICE
        // 状态既不是 UP 也不是 OUT_OF_SERVICE（即 DOWN/STARTING/UNKNOWN）→ 直接采纳
        if ((!InstanceInfo.InstanceStatus.UP.equals(instanceInfo.getStatus()))
                && (!InstanceInfo.InstanceStatus.OUT_OF_SERVICE.equals(instanceInfo.getStatus()))) {
            logger.debug("Trusting the instance status {} from replica or instance for instance {}",
                    instanceInfo.getStatus(), instanceInfo.getId());
            return StatusOverrideResult.matchingStatus(instanceInfo.getStatus());
        }
        // UP / OUT_OF_SERVICE → 不匹配，交给下一个规则判断
        return StatusOverrideResult.NO_MATCH;
    }

    @Override
    public String toString() {
        return DownOrStartingRule.class.getName();
    }
}
