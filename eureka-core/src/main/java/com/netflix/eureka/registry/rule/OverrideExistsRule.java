package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This rule checks to see if we have overrides for an instance and if we do then we return those.
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
public class OverrideExistsRule implements InstanceStatusOverrideRule {

    private static final Logger logger = LoggerFactory.getLogger(OverrideExistsRule.class);

    private Map<String, InstanceInfo.InstanceStatus> statusOverrides;

    public OverrideExistsRule(Map<String, InstanceInfo.InstanceStatus> statusOverrides) {
        this.statusOverrides = statusOverrides;
    }

    // 【规则②：运维覆盖优先】检查 overriddenInstanceStatusMap 中是否有该实例的覆盖状态。
    // 业务场景：运维通过 PUT /v2/apps/{app}/{id}/status?value=OUT_OF_SERVICE 把实例摘流
    // （灰度发布/故障隔离），覆盖状态存入 map。之后无论实例怎么上报 UP，
    // 这条规则都会用覆盖状态压制它——直到运维 DELETE 该覆盖状态为止。
    // 这就是"运维摘流后实例不会因为心跳/重新注册而自动恢复 UP"的原因
    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo, Lease<InstanceInfo> existingLease, boolean isReplication) {
        InstanceInfo.InstanceStatus overridden = statusOverrides.get(instanceInfo.getId());
        // If there are instance specific overrides, then they win - otherwise the ASG status
        if (overridden != null) {
            logger.debug("The instance specific override for instance {} and the value is {}",
                    instanceInfo.getId(), overridden.name());
            // 存在覆盖状态 → 用覆盖状态作为实例的最终状态
            return StatusOverrideResult.matchingStatus(overridden);
        }
        // 没有覆盖 → 交给下一个规则
        return StatusOverrideResult.NO_MATCH;
    }

    @Override
    public String toString() {
        return OverrideExistsRule.class.getName();
    }

}
