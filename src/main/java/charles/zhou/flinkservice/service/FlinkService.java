package charles.zhou.flinkservice.service;

import charles.zhou.flinkservice.entity.User;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
public class FlinkService {

    @Autowired
    private UserService userService;

    public void startFlinkJob() throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟大规模数据
        List<String> largeData = Arrays.asList(
                "user1,user1@example.com",
                "user2,user2@example.com",
                "user3,user3@example.com",
                "user4,user4@example.com",
                "user5,user5@example.com"
        );

        // 从集合中创建数据流，模拟实时数据输入
        DataStream<String> inputStream = env.fromCollection(largeData);

        // 1. 处理大规模数据和实时性
        // 使用 map 算子处理数据，模拟实时处理
        DataStream<Tuple2<String, String>> userStream = inputStream.map(new MapFunction<String, Tuple2<String, String>>() {
			private static final long serialVersionUID = 7010226868004715435L;

			@Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] parts = value.split(",");
                return Tuple2.of(parts[0], parts[1]);
            }
        });

        // 2. 分布式处理（Flink 会自动处理分布式，这里主要展示后续的处理逻辑）
        // 对用户名进行分组，模拟分布式计算
        DataStream<Tuple2<String, Integer>> groupedStream = userStream
               .map(tuple -> Tuple2.of(tuple.f0, 1))
               .keyBy(tuple -> tuple.f0)
               .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
				private static final long serialVersionUID = 8135915312376921784L;

					@Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        // 3. 状态管理
        // 统计每个用户出现的次数，并使用状态管理
        DataStream<Tuple2<String, Integer>> statefulStream = userStream
               .keyBy(tuple -> tuple.f0)
               .process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Integer>>() {
				private static final long serialVersionUID = 234591330271143183L;
					private transient ValueState<Integer> countState;

                    @SuppressWarnings("deprecation")
					@Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> descriptor =
                                new ValueStateDescriptor<>("count", Integer.class, 0);
                        countState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer count = countState.value();
                        count = count == null ? 1 : count + 1;
                        countState.update(count);
                        out.collect(Tuple2.of(value.f0, count));
                    }
                });

        // 4. 容错机制（Flink 会自动处理容错，这里主要展示整体流程）
        // 可以通过设置检查点来启用容错机制
        env.enableCheckpointing(5000); // 每 5 秒进行一次检查点

        // 5. 丰富的算子和函数库
        // 过滤出包含 "user1" 的数据
        DataStream<Tuple2<String, String>> filteredStream = userStream.filter(tuple -> tuple.f0.contains("user1"));

        // 将过滤结果插入数据库
        filteredStream.map(new MapFunction<Tuple2<String, String>, User>() {
			private static final long serialVersionUID = 2189455391178129240L;

			@Override
            public User map(Tuple2<String, String> value) throws Exception {
                User user = new User();
                user.setName(value.f0);
                user.setEmail(value.f1);
                return user;
            }
        }).addSink(new SinkFunction<User>() {
			private static final long serialVersionUID = 1L;

			@Override
            public void invoke(User value, Context context) throws Exception {
				userService.saveOrUpdate(value);
            }
        });

        // 打印结果
        groupedStream.print("Grouped Result");
        statefulStream.print("Stateful Result");
        filteredStream.print("Filtered Result");

        // 执行任务
        env.execute("Flink Features Demo");
    }
}    
