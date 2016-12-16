package net.chocolapod.sample.akka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class ActorSample {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ActorSystem actorSystem = ActorSystem.create("ActorSample");

        // Actor を作る
        ActorRef actor01 = actorSystem.actorOf(Props.create(MyActor.class, "MyActor01"), "MyActor01");
        ActorRef actor02 = actorSystem.actorOf(Props.create(MyActor.class, "MyActor02", actor01), "MyActor02");
        ActorRef actor03 = actorSystem.actorOf(Props.create(MyActor.class, "MyActor03", actor02), "MyActor03");

        // Actor を実行し、返り値をまつ
        Timeout timeout = new Timeout(Duration.create(60, "seconds"));
        Future<Object> future = Patterns.ask(actor03, new RequestChunk("chunk"), timeout);

        // Actor の処理が終わるまで待つ
        CompletableFuture<Object> completionStage = FutureConverters.toJava(future).toCompletableFuture();
        Object result = completionStage.toCompletableFuture().get();

        System.out.println(result);

        actorSystem.terminate();
    }

    public static class MyActor extends UntypedActor {
        private final String name;
        private final ActorRef nextActor;
        private ActorRef previousActor;

        public MyActor(String name) {
            this(name, null);
        }

        public MyActor(String name, ActorRef nextActor) {
            this.name = name;
            this.nextActor = nextActor;
        }

        @Override
        public void onReceive(Object message) throws Throwable {
            if (message instanceof RequestChunk) {
                previousActor = getSender();
                if (nextActor != null) {
                    // 後続の Actor に流す
                    nextActor.tell(toRequestChunk(message), getSelf());
                } else {
                    // 前方の Actor に戻す
                    previousActor.tell(toResponseChunk(message), getSelf());
                }
            } else if (message instanceof ResponseChunk) {
                // 前方の Actor に戻す
                previousActor.tell(toResponseChunk(message), getSelf());
            } else {
                unhandled(message);
            }
        }

        private ResponseChunk toResponseChunk(Object message) {
            return new ResponseChunk(message.toString() + "-" + name);
        }

        private RequestChunk toRequestChunk(Object message) {
            return new RequestChunk(message.toString() + "-" + name);
        }
    }

    public static class RequestChunk {
        private final String message;

        public RequestChunk(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return message;
        }
    }

    public static class ResponseChunk {
        private final String message;

        public ResponseChunk(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return message;
        }
    }

}
