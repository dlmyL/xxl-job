package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * <h1>哈希一致性路由策略</h1>
 * 分组下机器地址相同，不同JOB均匀散列在不同机器上，保证分组下机器分配JOB平均；且每个JOB固定调度其中一台机器；
 * a、virtual node：解决不均衡问题
 * b、hash method replace hashCode：String的hashCode可能重复，需要进一步扩大hashCode的取值范围
 */
public class ExecutorRouteConsistentHash extends ExecutorRouter {

    /**
     * 哈希环上存储的地址容量限制
     */
    private static int VIRTUAL_NODE_NUM = 100;

    /**
     * <h2>MD5 散列的方式计算 hash 值</h2>
     */
    private static long hash(String key) {
        // md5 byte
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        byte[] keyBytes = null;
        try {
            keyBytes = key.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unknown string :" + key, e);
        }

        md5.update(keyBytes);
        byte[] digest = md5.digest();

        // hash code, Truncate to 32-bits
        long hashCode = ((long) (digest[3] & 0xFF) << 24)
                | ((long) (digest[2] & 0xFF) << 16)
                | ((long) (digest[1] & 0xFF) << 8)
                | (digest[0] & 0xFF);

        long truncateHashCode = hashCode & 0xffffffffL;
        return truncateHashCode;
    }

    /**
     * <h2>这个方法的整体逻辑很简单，就是先计算每一个执行器地址的 hash 值，然后再计算定时任务 ID
     * 的 hash 值，然后将定时任务的哈希值和执行器地址的哈希值做对比，获得距离定时任务 ID 哈希值
     * 最近的那个执行器地址就行了，当然，这里要稍微形象一点，定时任务的哈希值构成了一个圆环，按照
     * 顺时针的方向，找到里面定时任务 ID 的哈希值最近的那个哈希值即可，这里用到了 TreeMap 结构</h2>
     */
    public String hashJob(int jobId, List<String> addressList) {

        // ------A1------A2-------A3------
        // -----------J1------------------
        TreeMap<Long, String> addressRing = new TreeMap<Long, String>();
        for (String address : addressList) {
            for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
                // 计算执行器地址的哈希值
                long addressHash = hash("SHARD-" + address + "-NODE-" + i);
                // 把地址哈希值和地址放到 TreeMap 中
                addressRing.put(addressHash, address);
            }
        }
        // 计算定时任务 ID 的哈希值
        long jobHash = hash(String.valueOf(jobId));
        // TreeMap 的 tailMap 方法在这里很重要，这个方法会让内部键值对的键跟 jobHash 做比较
        // 比 jobHash 的值大的键，对应的键值对都会返回给用户
        // 这里得到的 lastRing 就相当于圆环上所有比定时任务哈希值大的哈希值了
        SortedMap<Long, String> lastRing = addressRing.tailMap(jobHash);
        if (!lastRing.isEmpty()) {
            // 如果不为空，取第一个值就行了，最接近定时任务哈希值就行
            return lastRing.get(lastRing.firstKey());
        }
        // 如果为空，就从 addressRing 中获取第一个执行器地址即可
        return addressRing.firstEntry().getValue();
    }

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = hashJob(triggerParam.getJobId(), addressList);
        return new ReturnT<>(address);
    }

}
