package com.teld.bdp.metric;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;

import lombok.Builder;
import lombok.Data;

public class MyMetricApp {
    public static void main(String[] args) throws Exception {
        Configuration localConfig = new Configuration();
        localConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localConfig);
        /**
         * 方便监控，将并行度设置为1
         */
        env.setParallelism(1);
        env.disableOperatorChaining();
        DataStreamSource<Entity> source = env.addSource(new MySourceFunction());

        /**
         * 1.为方便监控，指定name为MyMap
         * 2.方便演示，仅仅将结果Print
         */
        source.map(new MyMapFun()).name("MyMap").print();
        env.execute("MyMetricApp");
    }

    /**
     * 主处理逻辑模拟
     */
    private static class MyMapFun extends RichMapFunction<Entity, Entity> {
        private transient Counter myCounter;
        private transient ThirdServiceMock thirdService;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            thirdService = new ThirdServiceMock();
            /**
             * Step1: 注册一个单独的Group，也可以用默认的Group
             */
            MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("MyMetric");
            /**
             * Step2：注册一个Counter
             */
            this.myCounter = metricGroup.counter("MyCounter");
        }

        @Override
        public Entity map(Entity entity) throws Exception {
            String desc = thirdService.callService(entity.getName());
            entity.setDesc(desc);
            /**
             * Step3：对Couter加1
             */
            myCounter.inc();
            return entity;
        }
    }

    /**
     * 模拟数据源
     */
    public static class MySourceFunction extends RichSourceFunction<Entity> {
        @Override
        public void run(SourceContext<Entity> ctx) throws Exception {

            // region
            List<Entity> entityList = Arrays.asList(
                    Entity.builder().id("1").name("zhangsan").age(18).country("jinan").build(),
                    Entity.builder().id("2").name("zhangsan").age(19).country("jinan").build(),
                    Entity.builder().id("3").name("zhangsan").age(19).country("beijing").build(),
                    Entity.builder().id("4").name("lisi").age(20).country("jinan").build(),
                    Entity.builder().id("5").name("lisi").age(20).country("jinan").build(),
                    Entity.builder().id("6").name("lisi").age(21).country("beijing").build(),
                    Entity.builder().id("7").name("wangwu").age(20).country("jinan").build(),
                    Entity.builder().id("8").name("wangwu").age(20).country("jinan").build(),
                    Entity.builder().id("9").name("wangwu").age(21).country("beijing").build(),
                    Entity.builder().id("10").name("maliu").age(20).country("jinan").build(),
                    Entity.builder().id("11").name("maliu").age(20).country("jinan").build(),
                    Entity.builder().id("12").name("maliu").age(21).country("beijing").build(),
                    Entity.builder().id("13").name("zhouqi").age(20).country("jinan").build(),
                    Entity.builder().id("14").name("zhouqi").age(20).country("jinan").build(),
                    Entity.builder().id("15").name("zhouqi").age(21).country("beijing").build()
            );
            // endregion

            int cnt = 0;
            while (true) {
                cnt++;
                int index = cnt % entityList.size();
                Entity entity = entityList.get(index);
                entity.age = cnt;
                ctx.collect(entity);
                Thread.sleep(500L);
            }
        }

        @Override
        public void cancel() {
        }
    }

    @Data
    @Builder
    public static class Entity {
        private String id;
        private String name;
        private int age;
        private String country;
        private String desc;
    }
}
