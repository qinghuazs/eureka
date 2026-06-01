package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule takes an ordered list of rules and returns the result of the first match or the
 * result of the {@link AlwaysMatchInstanceStatusRule}.
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
// 【规则链组合器】责任链模式：按顺序执行规则，第一个"匹配"的规则决定实例的最终状态。
// 解决的业务问题：实例的状态有多个"声音"——
//   实例自己说（status）、运维说（覆盖状态）、服务端历史记录说（已有租约的状态），
// 当它们冲突时听谁的？规则链的顺序就是话语权的优先级：
//   标准链（PeerAwareInstanceRegistryImpl 构造）：
//     ① DownOrStartingRule    —— 实例自报故障(DOWN/STARTING)最可信，直接采纳
//     ② OverrideExistsRule    —— 运维设置过覆盖状态，听运维的
//     ③ LeaseExistsRule       —— 服务端已有 UP/OUT_OF_SERVICE 记录，保持现状
//     ④ AlwaysMatch（兜底）    —— 都不匹配时，用实例自报的状态
// 所有规则在注册（register）和续约（renew）时都会被执行
public class FirstMatchWinsCompositeRule implements InstanceStatusOverrideRule {

    private final InstanceStatusOverrideRule[] rules;
    private final InstanceStatusOverrideRule defaultRule;
    private final String compositeRuleName;

    public FirstMatchWinsCompositeRule(InstanceStatusOverrideRule... rules) {
        this.rules = rules;
        this.defaultRule = new AlwaysMatchInstanceStatusRule();
        // Let's build up and "cache" the rule name to be used by toString();
        List<String> ruleNames = new ArrayList<>(rules.length+1);
        for (int i = 0; i < rules.length; ++i) {
            ruleNames.add(rules[i].toString());
        }
        ruleNames.add(defaultRule.toString());
        compositeRuleName = ruleNames.toString();
    }

    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease,
                                      boolean isReplication) {
        for (int i = 0; i < this.rules.length; ++i) {
            StatusOverrideResult result = this.rules[i].apply(instanceInfo, existingLease, isReplication);
            if (result.matches()) {
                return result;
            }
        }
        return defaultRule.apply(instanceInfo, existingLease, isReplication);
    }

    @Override
    public String toString() {
        return this.compositeRuleName;
    }
}
