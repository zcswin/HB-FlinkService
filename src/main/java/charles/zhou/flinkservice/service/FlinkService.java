package charles.zhou.flinkservice.service;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class FlinkService {

    // 线程安全的结果列表（保留原有功能）
    private final List<Tuple2<String, Integer>> groupedResults = Collections.synchronizedList(new ArrayList<>());
    private final List<Tuple2<String, Integer>> statefulResults = Collections.synchronizedList(new ArrayList<>());
    private final List<Tuple2<String, String>> filteredResults = Collections.synchronizedList(new ArrayList<>());

    // 作业状态管理（保留原有功能）
    private final AtomicBoolean jobRunning = new AtomicBoolean(false);
    private final AtomicInteger progress = new AtomicInteger(0);
    private final AtomicReference<String> errorMessage = new AtomicReference<>(null);

    public CompletableFuture<Void> startFlinkJob() {
        return CompletableFuture.runAsync(() -> {
            try {
                jobRunning.set(true);
                progress.set(0);
                errorMessage.set(null);
                
                // 清空历史结果（避免多次运行数据累加）
                groupedResults.clear();
                statefulResults.clear();
                filteredResults.clear();

                // 创建执行环境（保留原有功能）
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(2); // 示例并行度（可根据需求调整）

                // 输入数据（保留原有功能）
                List<String> largeData = Arrays.asList(
                    "user1,user1@example.com",
                    "user2,user2@example.com",
                    "user1,user1_alt@example.com",
                    "user3,user3@example.com",
                    "user2,user2_alt@example.com",
                    "user1,user1_another@example.com"
                );

                // 1. 实时数据处理（显式实现MapFunction，避免类型推断错误）
                DataStream<String> inputStream = env.fromCollection(largeData);
                DataStream<Tuple2<String, String>> userStream = inputStream.map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] parts = value.split(",");
                        return Tuple2.of(parts[0], parts[1]);
                    }
                }).name("UserInfo_Map"); // 为算子命名（便于监控）

                // 2. 分组统计（显式实现MapFunction，确保类型安全）
                DataStream<Tuple2<String, Integer>> groupedStream = userStream
                    .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> map(Tuple2<String, String> tuple) throws Exception {
                            return Tuple2.of(tuple.f0, 1);
                        }
                    })
                    .keyBy(tuple -> tuple.f0)
                    .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                        }
                    }).name("UserCount_Reduce");

                groupedStream.addSink(new GroupedSinkFunction(groupedResults)).name("Grouped_Sink");

                // 3. 状态管理（优化状态初始化逻辑）
                DataStream<Tuple2<String, Integer>> statefulStream = userStream
                    .keyBy(tuple -> tuple.f0)
                    .process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Integer>>() {
                        private transient ValueState<Integer> countState;

                        @Override
                        public void open(Configuration parameters) throws Exception {
                            // 状态描述符默认值设为0，避免null判断
                            ValueStateDescriptor<Integer> descriptor = 
                                new ValueStateDescriptor<>("user-count", Integer.class, 0);
                            countState = getRuntimeContext().getState(descriptor);
                        }

                        @Override
                        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                            int count = countState.value() + 1; // 直接累加，状态默认值为0
                            countState.update(count);
                            out.collect(Tuple2.of(value.f0, count));
                        }
                    }).name("Stateful_Process");

                statefulStream.addSink(new StatefulSinkFunction(statefulResults)).name("Stateful_Sink");

                // 4. 容错机制（保留原有配置）
                env.enableCheckpointing(5000); // 每5秒生成检查点
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000); // 检查点最小间隔3秒

                // 5. 数据过滤（显式实现FilterFunction，明确过滤逻辑）
                DataStream<Tuple2<String, String>> filteredStream = userStream
                    .filter(new FilterFunction<Tuple2<String, String>>() {
                        @Override
                        public boolean filter(Tuple2<String, String> tuple) throws Exception {
                            return tuple.f0.contains("user1"); // 保留原有逻辑（模糊匹配）
                        }
                    }).name("User1_Filter");

                filteredStream.addSink(new FilteredSinkFunction(filteredResults)).name("Filtered_Sink");

                // 执行作业（阻塞直至完成，更新进度）
                env.execute("Flink Features Demo");
                progress.set(100); // 作业完成后进度设为100%

            } catch (Exception e) {
                e.printStackTrace();
                errorMessage.set("Job failed: " + e.getMessage());
            } finally {
                jobRunning.set(false);
            }
        });
    }

    // 结果获取方法（完整保留所有用户要求的方法）
    public List<Tuple2<String, Integer>> getGroupedResults() {
        return groupedResults;
    }

    public List<Tuple2<String, Integer>> getStatefulResults() {
        return statefulResults;
    }

    public List<Tuple2<String, String>> getFilteredResults() {
        return filteredResults;
    }

    public boolean isJobRunning() {
        return jobRunning.get();
    }

    public int getProgress() {
        return progress.get();
    }

    public String getErrorMessage() {
        return errorMessage.get();
    }

    // Sink函数（完整保留内部类实现，确保线程安全）
    private static class GroupedSinkFunction implements SinkFunction<Tuple2<String, Integer>> {
        private final List<Tuple2<String, Integer>> results;

        public GroupedSinkFunction(List<Tuple2<String, Integer>> results) {
            this.results = results;
        }

        @Override
        public void invoke(Tuple2<String, Integer> result, Context context) throws Exception {
            results.add(result); // 使用synchronizedList保证线程安全
        }
    }

    private static class StatefulSinkFunction implements SinkFunction<Tuple2<String, Integer>> {
        private final List<Tuple2<String, Integer>> results;

        public StatefulSinkFunction(List<Tuple2<String, Integer>> results) {
            this.results = results;
        }

        @Override
        public void invoke(Tuple2<String, Integer> result, Context context) throws Exception {
            results.add(result);
        }
    }

    private static class FilteredSinkFunction implements SinkFunction<Tuple2<String, String>> {
        private final List<Tuple2<String, String>> results;

        public FilteredSinkFunction(List<Tuple2<String, String>> results) {
            this.results = results;
        }

        @Override
        public void invoke(Tuple2<String, String> result, Context context) throws Exception {
            results.add(result);
        }
    }
}