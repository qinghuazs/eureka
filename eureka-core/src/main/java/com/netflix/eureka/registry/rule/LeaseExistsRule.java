package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This rule matches if we have an existing lease for the instance that is UP or OUT_OF_SERVICE.
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
public class LeaseExistsRule implements InstanceStatusOverrideRule {

    private static final Logger logger = LoggerFactory.getLogger(LeaseExistsRule.class);

    // 【规则③：服务端记录优先】客户端直接请求（非复制）时，若服务端已有该实例的
    // UP 或 OUT_OF_SERVICE 记录，保持服务端的记录不变。
    // 业务场景：实例重启后重新注册，自报状态是 STARTING→UP，但服务端记录里它是 OUT_OF_SERVICE
    // （比如运维之前摘过流但覆盖状态因为1小时过期被清理了），此时优先维持服务端已有状态，
    // 防止实例重新注册"洗掉"服务端的状态记录。
    // 注意只对非复制请求生效：复制请求带来的是 peer 节点的权威数据，不应被本地旧记录拦截
    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease,
                                      boolean isReplication) {
        // This is for backward compatibility until all applications have ASG
        // names, otherwise while starting up
        // the client status may override status replicated from other servers
        if (!isReplication) {
            InstanceInfo.InstanceStatus existingStatus = null;
            if (existingLease != null) {
                existingStatus = existingLease.getHolder().getStatus();
            }
            // Allow server to have its way when the status is UP or OUT_OF_SERVICE
            // 服务端已有 UP/OUT_OF_SERVICE 记录 → 沿用服务端记录
            if ((existingStatus != null)
                    && (InstanceInfo.InstanceStatus.OUT_OF_SERVICE.equals(existingStatus)
                    || InstanceInfo.InstanceStatus.UP.equals(existingStatus))) {
                logger.debug("There is already an existing lease with status {}  for instance {}",
                        existingLease.getHolder().getStatus().name(),
                        existingLease.getHolder().getId());
                return StatusOverrideResult.matchingStatus(existingLease.getHolder().getStatus());
            }
        }
        // 复制请求 / 无已有租约 / 旧状态非 UP/OUT_OF_SERVICE → 交给兜底规则
        return StatusOverrideResult.NO_MATCH;
    }

    @Override
    public String toString() {
        return LeaseExistsRule.class.getName();
    }
}
