package com.sxhs.realtime.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangJunWei
 * @程序功能： flink任务base类
 * @created_in 2021/4/16 17:06
 */
public abstract class BaseJob {

    protected static StreamExecutionEnvironment env;

    public BaseJob(){
    }

    static {
        initEnv();
    }

    private static void initEnv(){
        //获取flink流式处理执行环境
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        //重启机制，失败率重启机制，3分钟内重试三次，每过10秒重试一次
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.MINUTES),
            Time.of(10, TimeUnit.SECONDS)));
        //默认5分钟进行一次checkPoint
        env.enableCheckpointing(5 * 60 * 1000);
        //获取checkpoint配置
        CheckpointConfig config = env.getCheckpointConfig();
        //精准一次处理
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //配置checkpoint并行度，同一时间值允许进行一次检查点
        config.setMaxConcurrentCheckpoints(1);
        //默认配置checkpoint必须在5分钟内完成一次checkpoint，否则检查点终止
        config.setCheckpointTimeout(5 * 60 * 1000);
        //默认设置checkpoint最小时间间隔
        config.setMinPauseBetweenCheckpoints(5000);
        //在cancel任务时候，系统保留checkpoint
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //并行度
        env.setParallelism(1);
    }
}
