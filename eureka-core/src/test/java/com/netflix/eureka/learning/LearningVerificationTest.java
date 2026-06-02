package com.netflix.eureka.learning;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.appinfo.LeaseInfo;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.eureka.lease.Lease;
import com.netflix.eureka.registry.rule.AlwaysMatchInstanceStatusRule;
import com.netflix.eureka.registry.rule.DownOrStartingRule;
import com.netflix.eureka.registry.rule.FirstMatchWinsCompositeRule;
import com.netflix.eureka.registry.rule.LeaseExistsRule;
import com.netflix.eureka.registry.rule.OverrideExistsRule;
import com.netflix.eureka.registry.rule.StatusOverrideResult;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 学习验证测试：用可运行的代码验证 docs/ 目录各文档中的核心论断。
 *
 * 每个测试方法对应文档中的一个具体结论，测试通过 = 文档结论被代码证实。
 * 运行方式：
 *   ./gradlew -I /tmp/eureka-init.gradle :eureka-core:test --tests "*LearningVerificationTest"
 *
 * @see <a href="../../../../../../../docs/">docs/ 源码分析文档</a>
 */
public class LearningVerificationTest {

    // ==================================================================================
    // 论断一（文档02 心跳续约流程）：Lease.renew() 存在著名的 +duration bug，
    // 导致实际过期时间是配置值的 2 倍（默认 90s 配置 → 实际 180s 才过期）
    // ==================================================================================

    /**
     * 验证：续约后立刻检查，在 1 倍 duration 内不过期（这是符合预期的部分）
     */
    @Test
    public void lease_bug_part1_within_one_duration_not_expired() {
        InstanceInfo instance = createInstance("verify-lease-bug");
        int durationInSecs = 2; // 用 2 秒租约做实验,避免测试太慢
        Lease<InstanceInfo> lease = new Lease<>(instance, durationInSecs);

        lease.renew();
        // 续约后立即判断:必然未过期
        assertFalse("续约后立即检查不应过期", lease.isExpired());
    }

    /**
     * 验证 bug 本体：经过 1 倍 duration 后，租约"应该"过期但实际没有过期。
     *
     * 文档02 的论断：renew() 把 lastUpdateTimestamp 设置成了 now + duration（多加了一个 duration），
     * 而 isExpired 判断是 now > lastUpdateTimestamp + duration，
     * 所以实际过期时刻 = 续约时刻 + 2 * duration。
     */
    @Test
    public void lease_bug_part2_after_one_duration_still_not_expired() throws InterruptedException {
        InstanceInfo instance = createInstance("verify-lease-bug-2");
        int durationInSecs = 1; // 1 秒租约
        Lease<InstanceInfo> lease = new Lease<>(instance, durationInSecs);

        lease.renew();
        // 等待 1.5 倍 duration（如果没有 bug,此时应该已过期）
        Thread.sleep(1500);

        // bug 的存在使得此时仍未过期（实际要等 2 秒才过期）
        assertFalse("【bug验证】1.5倍duration后理论上应过期,但因+duration bug实际未过期",
                lease.isExpired());

        // 再等 1 秒（总共 2.5 秒 > 2*duration）,现在真的过期了
        Thread.sleep(1000);
        assertTrue("2.5倍duration后(超过2*duration)应已过期", lease.isExpired());
    }

    // ==================================================================================
    // 论断二（文档07 状态覆盖规则链）：规则链的优先级裁决
    // ==================================================================================

    /**
     * 验证：实例自报 DOWN 时，DownOrStartingRule 直接采纳（"坏消息直通"）
     */
    @Test
    public void rule_chain_down_status_is_trusted_directly() {
        DownOrStartingRule rule = new DownOrStartingRule();
        InstanceInfo downInstance = createInstanceWithStatus("down-instance", InstanceStatus.DOWN);

        StatusOverrideResult result = rule.apply(downInstance, null, false);

        assertTrue("实例自报DOWN应被规则匹配", result.matches());
        assertEquals("匹配结果应为DOWN", InstanceStatus.DOWN, result.status());
    }

    /**
     * 验证：实例自报 UP 时，DownOrStartingRule 不匹配（"好消息过审"），交给后续规则
     */
    @Test
    public void rule_chain_up_status_is_not_trusted_directly() {
        DownOrStartingRule rule = new DownOrStartingRule();
        InstanceInfo upInstance = createInstanceWithStatus("up-instance", InstanceStatus.UP);

        StatusOverrideResult result = rule.apply(upInstance, null, false);

        assertFalse("实例自报UP不应被DownOrStartingRule匹配(需要后续规则验证)", result.matches());
    }

    /**
     * 验证文档07核心论断：运维设置覆盖状态(OUT_OF_SERVICE)后，
     * 实例自报 UP 会被 OverrideExistsRule 压制——这就是"摘流后心跳改不回状态"的原因
     */
    @Test
    public void rule_chain_override_suppresses_instance_reported_up() {
        // 模拟运维操作:把实例设置为 OUT_OF_SERVICE（写入覆盖状态map）
        Map<String, InstanceStatus> overrides = new HashMap<>();
        InstanceInfo instance = createInstanceWithStatus("suppressed-instance", InstanceStatus.UP);
        overrides.put(instance.getId(), InstanceStatus.OUT_OF_SERVICE);

        // 组装与 PeerAwareInstanceRegistryImpl 相同的标准规则链
        FirstMatchWinsCompositeRule chain = new FirstMatchWinsCompositeRule(
                new DownOrStartingRule(),
                new OverrideExistsRule(overrides),
                new LeaseExistsRule());

        // 实例心跳/注册时自报 UP
        StatusOverrideResult result = chain.apply(instance, null, false);

        // 最终裁决:OUT_OF_SERVICE(运维意志压制实例自报)
        assertEquals("覆盖状态应压制实例自报的UP",
                InstanceStatus.OUT_OF_SERVICE, result.status());
    }

    /**
     * 验证：没有任何特殊情况时，兜底规则采纳实例自报状态
     */
    @Test
    public void rule_chain_fallback_accepts_instance_status() {
        FirstMatchWinsCompositeRule chain = new FirstMatchWinsCompositeRule(
                new DownOrStartingRule(),
                new OverrideExistsRule(new HashMap<String, InstanceStatus>()),
                new LeaseExistsRule());

        InstanceInfo upInstance = createInstanceWithStatus("normal-instance", InstanceStatus.UP);
        StatusOverrideResult result = chain.apply(upInstance, null, false);

        assertEquals("无覆盖无租约时,应兜底采纳实例自报的UP", InstanceStatus.UP, result.status());
    }

    /**
     * 验证文档07的"坑"：覆盖状态被删除后，LeaseExistsRule 仍会维持服务端已有的 OUT_OF_SERVICE 记录
     * （这就是"等覆盖状态过期不能让实例恢复"的原因）
     */
    @Test
    public void rule_chain_lease_exists_keeps_old_status_after_override_removed() {
        // 服务端已有租约,租约里的状态是 OUT_OF_SERVICE（之前被运维摘过流）
        InstanceInfo leaseHolder = createInstanceWithStatus("lease-holder", InstanceStatus.OUT_OF_SERVICE);
        Lease<InstanceInfo> existingLease = new Lease<>(leaseHolder, 90);

        // 覆盖状态map是空的（模拟覆盖状态已被删除/过期）
        FirstMatchWinsCompositeRule chain = new FirstMatchWinsCompositeRule(
                new DownOrStartingRule(),
                new OverrideExistsRule(new HashMap<String, InstanceStatus>()),
                new LeaseExistsRule());

        // 实例自报 UP（希望恢复服务）
        InstanceInfo instance = createInstanceWithStatus("lease-holder", InstanceStatus.UP);
        StatusOverrideResult result = chain.apply(instance, existingLease, false);

        // 仍然是 OUT_OF_SERVICE！LeaseExistsRule 维持了服务端记录
        assertEquals("【坑验证】覆盖删除后,LeaseExistsRule仍维持租约中的OUT_OF_SERVICE",
                InstanceStatus.OUT_OF_SERVICE, result.status());
    }

    // ==================================================================================
    // 论断三（文档09 数据模型）：Applications 的一致性 hashcode 算法
    // 格式 = 按状态名排序的 "状态_数量_" 拼接
    // ==================================================================================

    /**
     * 验证 hashcode 格式：2个UP + 1个DOWN → "DOWN_1_UP_2_"（按状态名字典序）
     */
    @Test
    public void hashcode_format_is_status_count_pairs_sorted_by_name() {
        Applications applications = new Applications();
        Application app = new Application("HASHCODE-TEST-APP");

        app.addInstance(createInstanceWithStatus("up-1", InstanceStatus.UP));
        app.addInstance(createInstanceWithStatus("up-2", InstanceStatus.UP));
        app.addInstance(createInstanceWithStatus("down-1", InstanceStatus.DOWN));
        applications.addApplication(app);

        String hashCode = applications.getReconcileHashCode();

        // 文档09论断:格式为 "状态_数量_" 按状态名排序拼接（DOWN在UP前面）
        assertEquals("hashcode格式应为按状态名排序的 状态_数量_ 拼接",
                "DOWN_1_UP_2_", hashCode);
    }

    /**
     * 验证 hashcode 的"盲区"（文档09提到的局限性）：
     * 实例内容变化但状态分布不变时，hashcode 完全相同——它只对数量敏感
     */
    @Test
    public void hashcode_is_blind_to_content_changes() {
        Applications apps1 = new Applications();
        Application appA = new Application("APP-A");
        appA.addInstance(createInstanceWithStatus("instance-1", InstanceStatus.UP));
        apps1.addApplication(appA);

        Applications apps2 = new Applications();
        Application appB = new Application("APP-B");   // 完全不同的应用
        appB.addInstance(createInstanceWithStatus("instance-999", InstanceStatus.UP));  // 完全不同的实例
        apps2.addApplication(appB);

        // 两份完全不同的注册表,只因"都是1个UP",hashcode相同
        assertEquals("【局限性验证】不同内容但相同状态分布的注册表,hashcode相同",
                apps1.getReconcileHashCode(), apps2.getReconcileHashCode());
    }

    // ==================================================================================
    // 论断四（文档04 自我保护）：阈值公式 = 客户端数 × (60/续约间隔) × 阈值百分比
    // 这里直接验证公式数学,不依赖 Server 环境
    // ==================================================================================

    /**
     * 验证自我保护阈值公式：100个客户端、30s心跳间隔、0.85阈值 → 阈值=170
     */
    @Test
    public void self_preservation_threshold_formula() {
        int expectedNumberOfClientsSendingRenews = 100;
        int expectedClientRenewalIntervalSeconds = 30;   // 默认心跳间隔
        double renewalPercentThreshold = 0.85;            // 默认阈值百分比

        // 这就是 AbstractInstanceRegistry.updateRenewsPerMinThreshold() 的公式
        int threshold = (int) (expectedNumberOfClientsSendingRenews
                * (60.0 / expectedClientRenewalIntervalSeconds)
                * renewalPercentThreshold);

        assertEquals("100客户端×每分钟2次心跳×0.85 = 170", 170, threshold);
    }

    /**
     * 验证文档04的论断：测试环境实例少时自我保护极易误触发。
     * 3个实例的阈值是5,任何一个实例心跳异常(每分钟心跳从6变4)都会触发保护
     */
    @Test
    public void self_preservation_is_sensitive_with_few_instances() {
        int threshold = (int) (3 * (60.0 / 30) * 0.85);   // 3个实例的阈值
        assertEquals("3个实例的阈值为5", 5, threshold);

        int renewsWhenOneInstanceDown = 2 * 2;             // 只剩2个实例正常心跳=每分钟4次
        assertTrue("一个实例异常即触发自我保护(4 <= 5)", renewsWhenOneInstanceDown <= threshold);
    }

    // ==================================================================================
    // 工具方法
    // ==================================================================================

    private InstanceInfo createInstance(String id) {
        return createInstanceWithStatus(id, InstanceStatus.UP);
    }

    private InstanceInfo createInstanceWithStatus(String id, InstanceStatus status) {
        return InstanceInfo.Builder.newBuilder()
                .setInstanceId(id)
                .setAppName("LEARNING-TEST-APP")
                .setHostName(id + ".test.local")
                .setIPAddr("127.0.0.1")
                .setPort(8080)
                .setDataCenterInfo(new com.netflix.appinfo.MyDataCenterInfo(
                        com.netflix.appinfo.DataCenterInfo.Name.MyOwn))
                .setLeaseInfo(LeaseInfo.Builder.newBuilder().build())
                .setStatus(status)
                .build();
    }
}
