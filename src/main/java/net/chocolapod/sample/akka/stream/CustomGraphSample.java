package net.chocolapod.sample.akka.stream;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.ClosedShape;
import akka.stream.FanInShape2;
import akka.stream.FanOutShape2;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Materializer;
import akka.stream.Outlet;
import akka.stream.SinkShape;
import akka.stream.SourceShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.GraphStageWithMaterializedValue;
import scala.Tuple2;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;
import scala.concurrent.Promise;

/**
 * Akka Stream で独自 Graph を実装するサンプル
 */
public class CustomGraphSample {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        final ActorSystem system = ActorSystem.create("CustomGraphSample");
        final Materializer materializer = ActorMaterializer.create(system);

        /**
         * Graph を定義する
         */

        // Source を作る

        final MySource source = new MySource();

        // Sink を作る

        final Sink<String, Future<List<String>>> sink = Sink.fromGraph(new MySink());

        // Flow を作る

        final Flow<String, String, NotUsed> f1 = Flow.fromGraph(new MyFlow("f1"));
        final Flow<String, String, NotUsed> f2 = Flow.fromGraph(new MyFlow("f2"));
        final Flow<String, String, NotUsed> f3 = Flow.fromGraph(new MyFlow("f3"));
        final Flow<String, String, NotUsed> f4 = Flow.fromGraph(new MyFlow("f4"));
        final MyFanOut fanOut = new MyFanOut();
        final MyFanIn fanIn = new MyFanIn();

        /**
         * RunnableGraph を定義する
         */
        final RunnableGraph<Future<List<String>>> runnableGraph = RunnableGraph.fromGraph(GraphDSL.create(sink, (builder, out) -> {
            final Outlet<String> in = builder.add(source).out();
            final FanOutShape2<String, String, String> balance = builder.add(fanOut);
            final FanInShape2<String, String, String> merge = builder.add(fanIn);

            // Broadcast したのちに合流させる
            builder.from(in).via(builder.add(f1.async())).toInlet(balance.in());

            builder.from(balance.out0()).via(builder.add(f2.async())).toInlet(merge.in0());
            builder.from(balance.out1()).via(builder.add(f3.async())).toInlet(merge.in1());

            builder.from(merge.out()).via(builder.add(f4.async())).to(out);

            return ClosedShape.getInstance();
        }));

        /**
         * RunnableGraph を実行する
         */
        Future<List<String>> future = runnableGraph.run(materializer);

        /**
         * RunnableGraph の実行を待って結果を取得する
         */
        CompletableFuture<List<String>> completionStage = FutureConverters.toJava(future).toCompletableFuture();
        List<String> result = new ArrayList<>(completionStage.toCompletableFuture().get());

        Collections.sort(result);

        System.out.println("*** Result ***");
        System.out.println(String.join("\n", result));

        system.terminate();
    }

    private static void log(String flowName, String chunk) {
        System.out.println(String.format("%s %s(%d) pass %s", LocalDateTime.now(), flowName, Thread.currentThread().getId(), chunk.substring(0, "chunk01".length())));
    }

    private static void sleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (Exception e) {
        };
    }

    public static class MySource extends GraphStage<SourceShape<String>> {
        private final Outlet<String> out = Outlet.create("MySource.out");
        private final SourceShape<String> shape = SourceShape.of(out);

        @Override
        public SourceShape<String> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) {
            return new GraphStageLogic(shape()) {
                // Graph の state はすべて GraphStageLogic 内に持つこと
                private final List<String> chunks = new ArrayList<>(Arrays.asList("chunk01", "chunk02", "chunk03", "chunk04", "chunk05"));

                {
                    setHandler(out, new AbstractOutHandler() {
                        @Override
                        public void onPull() throws Exception {
                            if (chunks.isEmpty()) {
                                // 終わったら閉じる
                                complete(out);
                            } else {
                                push(out, chunks.remove(0));
                                // すでに別の要素を書き込んでいます、と言われたら emit() を検討するとよい
                            }
                        }
                    });
                }
            };
        }
    }

    // GraphStage を継承すると返り値のない Graph になる
    public static class MySink extends GraphStageWithMaterializedValue<SinkShape<String>, Future<List<String>>> {
        private final Inlet<String> in = Inlet.create("MySink.in");
        private final SinkShape<String> shape = SinkShape.of(in);

        @Override
        public SinkShape<String> shape() {
            return shape;
        }

        @Override
        public Tuple2<GraphStageLogic, Future<List<String>>> createLogicAndMaterializedValue(Attributes inheritedAttributes) throws Exception {
            final Promise<List<String>> promise = Futures.promise();
            final GraphStageLogic graphStageLogic = new GraphStageLogic(shape()) {
                private final List<String> chunks = new ArrayList<>();

                @Override
                public void preStart() {
                    // 最初に Sink からも要求をしておく
                    pull(in);
                }

                {
                    setHandler(in, new AbstractInHandler() {
                        @Override
                        public void onUpstreamFinish() throws Exception {
                            // 上流が終了した時の処理を行う
                            promise.success(chunks);

                            // 最後に呼び出す
                            super.onUpstreamFinish();
                        }

                        @Override
                        public void onUpstreamFailure(Throwable ex) throws Exception {
                            // 上流が異常終了した時の処理を行う
                            promise.success(chunks);

                            // 最後に呼び出す
                            super.onUpstreamFailure(ex);
                        }

                        @Override
                        public void onPush() throws Exception {
                            String chunk = grab(in);

                            chunks.add(chunk);

                            if ((!hasBeenPulled(in)) && (!isClosed(in))) {
                                pull(in);
                            }
                        }
                    });
                }
            };

            return new Tuple2<>(graphStageLogic, promise.future());
        }
    }

    public static class MyFlow extends GraphStage<FlowShape<String, String>> {
        private final Inlet<String> in;
        private final Outlet<String> out;
        private final FlowShape<String, String> shape;
        private final String name;

        public MyFlow(String name) {
            this.name = name;
            this.in = Inlet.create("MyFlow." + name + ".in");
            this.out = Outlet.create("MyFlow." + name + ".out");
            this.shape = new FlowShape<>(in, out);
        }

        @Override
        public FlowShape<String, String> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
            return new GraphStageLogic(shape()) {
                {
                    setHandler(in, new AbstractInHandler() {
                        @Override
                        public void onPush() throws Exception {
                            String chunk = grab(in);

                            log(name, chunk);
                            sleep(100);

                            chunk += ("-" + name);

                            push(out, chunk);
                        }
                    });
                    setHandler(out, new AbstractOutHandler() {
                        @Override
                        public void onPull() throws Exception {
                            if ((!hasBeenPulled(in)) && (!isClosed(in))) {
                                pull(in);
                            }
                        }
                    });
                }
            };
        }
    }

    public static class MyFanOut extends GraphStage<FanOutShape2<String, String, String>> {
        private final Inlet<String> in = Inlet.create("MyFanOut.in");
        private final Outlet<String> out1 = Outlet.create("MyFanOut.out1");
        private final Outlet<String> out2 = Outlet.create("MyFanOut.out2");
        private final FanOutShape2<String, String, String> shape = new FanOutShape2<>(in, out1, out2);

        @Override
        public FanOutShape2<String, String, String> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
            return new GraphStageLogic(shape()) {
                {
                    setHandler(in, new AbstractInHandler() {
                        @Override
                        public void onPush() throws Exception {
                            String chunk = grab(in);

                            // chunk の内容によって流す先をきりかえる
                            if (0 <= chunk.indexOf("3")) {
                                push(out1, chunk);
                            } else {
                                push(out2, chunk);
                            }
                        }
                    });
                    for (Outlet<String> out : Arrays.asList(out1, out2)) {
                        setHandler(out, new AbstractOutHandler() {
                            @Override
                            public void onPull() throws Exception {
                                if ((!hasBeenPulled(in)) && (!isClosed(in))) {
                                    pull(in);
                                }
                            }
                        });
                    }
                }
            };
        }
    }

    public static class MyFanIn extends GraphStage<FanInShape2<String, String, String>> {
        private final Inlet<String> in1 = Inlet.create("MyFanIn.in1");
        private final Inlet<String> in2 = Inlet.create("MyFanIn.in2");
        private final Outlet<String> out = Outlet.create("MyFanIn.out");
        private final FanInShape2<String, String, String> shape = new FanInShape2<>(in1, in2, out);

        @Override
        public FanInShape2<String, String, String> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
            return new GraphStageLogic(shape()) {
                {
                    for (Inlet<String> in : Arrays.asList(in1, in2)) {
                        setHandler(in, new AbstractInHandler() {
                            @Override
                            public void onPush() throws Exception {
                                String chunk = grab(in);

                                push(out, chunk);
                            }
                        });
                    }
                    setHandler(out, new AbstractOutHandler() {
                        @Override
                        public void onPull() throws Exception {
                            for (Inlet<String> in : Arrays.asList(in1, in2)) {
                                if ((!hasBeenPulled(in)) && (!isClosed(in))) {
                                    pull(in);
                                }
                            }
                        }
                    });
                }
            };
        }
    }

}
