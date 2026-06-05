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
import static org.junit.Assert.assertNotEquals;
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
    // 论断五（文档04 剔除上限）：单次剔除上限 = 注册表大小 - (int)(大小 × 0.85)
    // 文档反复说"×15%"，但因 int 截断，"15%"只在 size=100 时精确成立，规模越小占比越大。
    // 见 AbstractInstanceRegistry.java:683-685
    // ==================================================================================

    /**
     * 验证剔除上限公式，并揭示文档"15%"说法的边界：int 截断使它只在 size=100 时恰好等于 15%。
     */
    @Test
    public void eviction_limit_is_size_minus_floor_of_85_percent() {
        // size=100：恰好剔除 15 个（= 15%），这正是文档"×15%"说法的来源
        assertEquals("100 个实例单次最多剔除 15 个", 15, evictionLimit(100));

        // 但 int 截断让"15%"只是 size=100 的巧合——小规模实际占比明显更大：
        assertEquals("10 个实例上限是 2(=20%,并非15%)", 2, evictionLimit(10));
        assertEquals("7 个实例上限是 2(≈28.6%)", 2, evictionLimit(7));
        assertEquals("1 个实例上限是 1(=100%,单实例注册表几乎无上限保护)", 1, evictionLimit(1));
    }

    // ==================================================================================
    // 论断六（文档04 补偿时间）：getCompensationTimeMs = 实际间隔 - 配置间隔(60s)，取非负。
    // 修正 Full GC / 时钟漂移导致剔除任务延迟执行时的大面积误剔除。
    // 见 AbstractInstanceRegistry.java:1368-1369
    // ==================================================================================

    /**
     * 验证补偿时间公式的三种情形，重点是"负值钳为 0"——绝不能反向缩短租约。
     */
    @Test
    public void compensation_time_is_elapsed_minus_interval_clamped_to_zero() {
        long interval = 60_000L;  // 默认剔除周期 60s
        // 发生 30s 的 Full GC,本次任务实际间隔 90s 才执行 → 补偿 30s
        assertEquals("实际间隔90s,配置60s → 补偿30s", 30_000L, compensationTimeMs(90_000L, interval));
        // 正常按时执行 → 不补偿
        assertEquals("实际间隔=配置间隔 → 补偿0", 0L, compensationTimeMs(60_000L, interval));
        // 提前执行(时钟回拨) → 补偿钳为0,绝不返回负数(否则会反向缩短租约导致误剔除)
        assertEquals("实际间隔小于配置 → 补偿钳为0,不为负", 0L, compensationTimeMs(50_000L, interval));
    }

    // ==================================================================================
    // 论断七（文档04 主动下线即时生效）：Lease.cancel() 后立即过期，与续约时间无关。
    // 见 Lease.java:86-90（evictionTimestamp）、:132-133（isExpired 的 evictionTimestamp>0 分支）
    // ==================================================================================

    /**
     * 验证 cancel() 走的是 evictionTimestamp>0 分支，立即过期——这是"主动下线即时生效"
     * 与"被动剔除靠租约超时(默认 180s)"两套机制的分界。
     */
    @Test
    public void lease_cancel_expires_immediately_regardless_of_renewal() {
        InstanceInfo instance = createInstance("cancel-instance");
        Lease<InstanceInfo> lease = new Lease<>(instance, 90);  // 90s 租约
        lease.renew();
        assertFalse("刚续约,远未到期", lease.isExpired());

        lease.cancel();  // 主动下线
        // evictionTimestamp>0 这一支让 isExpired 立即为 true,不必等 2*duration 的租约超时
        assertTrue("cancel()后应立即过期(主动下线即时生效)", lease.isExpired());
    }

    // ==================================================================================
    // 论断八（文档04 补偿语义）：isExpired(additionalLeaseMs) 把过期时刻整体后移，
    // 即剔除任务延迟时给所有实例"续上"进程暂停的那段时间。见 Lease.java:132-133
    // ==================================================================================

    /**
     * 验证 additionalLeaseMs 的方向：同一条已越过过期时刻的租约，加上正补偿后又被判为未过期。
     */
    @Test
    public void additional_lease_ms_postpones_expiry_for_gc_compensation() throws InterruptedException {
        InstanceInfo instance = createInstance("compensation-instance");
        Lease<InstanceInfo> lease = new Lease<>(instance, 1);  // 1s 租约(因 +duration bug 实际 2s 才过期)
        lease.renew();
        Thread.sleep(2100);  // 越过 2*duration,无补偿时已过期

        assertTrue("无补偿:已越过实际过期时刻", lease.isExpired(0L));
        assertFalse("加 5s 补偿:过期时刻整体后移,又判定为未过期", lease.isExpired(5_000L));
    }

    // ==================================================================================
    // 论断九（文档03 增量对账触发）：客户端本地合并后算出的 reconcileHashCode 与服务端返回的
    // appsHashCode 一致则不全量、不一致则触发全量兜底——这是 hashcode 机制存在的全部理由。
    // 见 DiscoveryClient 中 if(!reconcileHashCode.equals(delta.getAppsHashCode())) 的判定
    // ==================================================================================

    /**
     * 用纯模型对象验证"hashcode 一致=不全量、不一致=全量"的决策点：
     * 状态计数一致即视为合并正确（无需逐实例比对），漏删一条会立即被指纹发现。
     */
    @Test
    public void reconcile_hashcode_decides_whether_full_fetch_is_triggered() {
        // 服务端基线:2 个 UP + 1 个 DOWN
        Applications serverSide = appsWith("RECON-APP", 2, 1);
        String serverHash = serverSide.getReconcileHashCode();

        // 客户端"正确"增量合并后:状态计数一致(实例 id 不同也无所谓)→ hashcode 相等 → 不触发全量
        Applications correctlyMerged = appsWith("RECON-APP", 2, 1);
        assertEquals("合并正确,hashcode一致 → 不触发全量对账",
                serverHash, correctlyMerged.getReconcileHashCode());

        // 客户端"漏掉一条 DELETE"导致多留一个 DOWN 实例:计数不一致 → hashcode 不等 → 触发全量
        Applications staleMerged = appsWith("RECON-APP", 2, 2);
        assertNotEquals("合并漏删,hashcode不一致 → 触发全量对账修复",
                serverHash, staleMerged.getReconcileHashCode());
    }

    // ==================================================================================
    // 工具方法
    // ==================================================================================

    /** 复刻 AbstractInstanceRegistry.java:683-685 的单次剔除上限公式（默认阈值 0.85）。 */
    private static int evictionLimit(int registrySize) {
        int registrySizeThreshold = (int) (registrySize * 0.85);
        return registrySize - registrySizeThreshold;
    }

    /** 复刻 AbstractInstanceRegistry.java:1368-1369 的补偿时间公式（负值钳为 0）。 */
    private static long compensationTimeMs(long elapsedMs, long intervalMs) {
        long compensationTime = elapsedMs - intervalMs;
        return compensationTime <= 0L ? 0L : compensationTime;
    }

    /** 构造一个含 upCount 个 UP、downCount 个 DOWN 实例的 Applications，用于 hashcode 对账验证。 */
    private Applications appsWith(String appName, int upCount, int downCount) {
        Applications applications = new Applications();
        Application app = new Application(appName);
        for (int i = 0; i < upCount; i++) {
            app.addInstance(createInstanceWithStatus(appName + "-up-" + i, InstanceStatus.UP));
        }
        for (int i = 0; i < downCount; i++) {
            app.addInstance(createInstanceWithStatus(appName + "-down-" + i, InstanceStatus.DOWN));
        }
        applications.addApplication(app);
        return applications;
    }

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
