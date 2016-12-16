package net.chocolapod.sample.akka.stream;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.Materializer;
import akka.stream.Outlet;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

/**
 * Akka Stream の RunnableGraph のサンプル
 * 
 * 参考: http://doc.akka.io/docs/akka/2.4/java/stream/stream-graphs.html
 */
public class RunnableGraphSample {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        final ActorSystem system = ActorSystem.create("RunnableGraphSample");
        final Materializer materializer = ActorMaterializer.create(system);

        /**
         * Graph を定義する
         */

        // Source を作る

        final Source<String, NotUsed> in = Source.from(Arrays.asList("chunk01", "chunk02", "chunk03", "chunk04", "chunk05"));

        // Sink を作る

        final Sink<List<String>, CompletionStage<List<String>>> sink = Sink.head();

        // Flow を作る

        final Flow<String, String, NotUsed> f1 = Flow.of(String.class).map(chunk -> {
            log("f1", chunk);
            sleep(100);

            return chunk + "-f1";
        });

        final Flow<String, String, NotUsed> f2 = Flow.of(String.class).map(chunk -> {
            log("f2", chunk);
            sleep(300);

            return chunk + "-f2";
        });

        final Flow<String, String, NotUsed> f3 = Flow.of(String.class).map(chunk -> {
            log("f3", chunk);
            sleep(500);

            return chunk + "-f3";
        });

        final Flow<String, String, NotUsed> f4 = Flow.of(String.class).map(chunk -> {
            log("f4", chunk);
            sleep(100);

            return chunk + "-f4";
        });

        final Flow<String, String, NotUsed> f5 = Flow.of(String.class).map(chunk -> {
            log("f5", chunk);
            sleep(1000);

            return chunk + "-f5";
        });

        final Flow<String, String, NotUsed> f6 = Flow.of(String.class).map(chunk -> {
            log("f6", chunk);
            sleep(300);

            return chunk + "-f6";
        });

        final Flow<String, String, NotUsed> f7 = Flow.of(String.class).map(chunk -> {
            log("f7", chunk);
            sleep(100);

            return chunk + "-f7";
        });

        /**
         * RunnableGraph を定義する
         */
        final RunnableGraph<CompletionStage<List<String>>> runnableGraph = RunnableGraph.fromGraph(GraphDSL.create(sink, (builder, out) -> {
            final Outlet<String> source = builder.add(in).out();
            // Broadcast は両方に流す（1 つの chunk が 2 つ流れる）
            final UniformFanOutShape<String, String> broadcast = builder.add(Broadcast.create(2));
            // Balance は片方に流す
            final UniformFanOutShape<String, String> balance = builder.add(Balance.create(2));
            // FanIn は複数を合流させる
            final UniformFanInShape<String, String> merge1 = builder.add(Merge.create(2));
            final UniformFanInShape<String, String> merge2 = builder.add(Merge.create(2));

            // Broadcast したのちに合流させる
            builder.from(source).via(builder.add(f1.async())).toFanOut(broadcast);

            builder.from(broadcast).via(builder.add(f2.async())).toFanIn(merge1);
            builder.from(broadcast).via(builder.add(f3.async())).toFanIn(merge1);

            // Balance したのちに合流させる
            builder.from(merge1).via(builder.add(f4.async())).toFanOut(balance);

            builder.from(balance).via(builder.add(f5.async())).toFanIn(merge2);
            builder.from(balance).via(builder.add(f6.async())).toFanIn(merge2);

            // 流れる String をまとめて List<String> にする
            builder.from(merge2).via(builder.add(f7.grouped(1000))).to(out);

            return ClosedShape.getInstance();
        }));

        /**
         * RunnableGraph を実行する
         */
        CompletionStage<List<String>> completionStage = runnableGraph.run(materializer);

        /**
         * RunnableGraph の実行を待って結果を取得する
         */
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
}
