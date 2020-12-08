package com.github.xy02.ritp_kt

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.plugins.RxJavaPlugins
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.PublishSubject
import io.vertx.core.*
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import ritp.Header
import ritp.Info
import java.text.SimpleDateFormat
import java.util.*

fun main(args: Array<String>) {
    val pubKey = Base64.getDecoder().decode("zxj0S8mMIE0QDeJqOMPSll0LJBZr0bnn7fdw+/fpuRY=")
    RxJavaPlugins.setErrorHandler { e -> println("RxJavaPlugins e:$e") }

//        var options = new HttpServerOptions()
//                .setMaxWebSocketFrameSize(1000000)
//                .addWebSocketSubProtocol("ritp");
//        var sockets = createWebSockets(options).flatMapSingle(Infras::parseToSocket);
    val wss = PublishSubject.create<ServerWebSocket>()
    val source = nioSocketsSource()
    val sockets = Observable.merge(
        nioServerSockets(source),
        wss.flatMapSingle { ws -> parseToSocket(ws) }
    ).doOnNext { println("new socket") }

    val myInfo = Info.newBuilder().setVersion("0.1").build()
//    val init = initWith(myInfo)
    val ctxs = init(myInfo, sockets,
        InitOptions(
            denyAnonymousApp = true,
            appPublicKeyMap = mapOf("someApp" to pubKey)
        )).doOnNext { println("new context") }
    ctxs.flatMap { ctx -> handleConnection(ctx.connection) }.subscribe()
    val vertx = Vertx.vertx(VertxOptions().setPreferNativeTransport(true))
//    val vertx = Vertx.vertx()
    vertx.deployVerticle(
//        Infras::class.java.name,
        {
            Infras(wss)
        },
        DeploymentOptions().setInstances(VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE),
        { event: AsyncResult<String?> ->
            if (event.succeeded()) {
                println("Server listening on port " + 8000)
            } else {
                println("Unable to start your application" + event.cause())
            }
        })
}

class Infras(private val wss: Observer<ServerWebSocket>) : AbstractVerticle() {

    override fun start(startPromise: Promise<Void?>) {
        val options = HttpServerOptions()
            .setMaxWebSocketFrameSize(1000000)

            .addWebSocketSubProtocol("ritp")
        val server = vertx.createHttpServer(options)
        server.webSocketHandler { ws: ServerWebSocket? ->
            println("onWs:" + Thread.currentThread().id)
            wss.onNext(ws)
        }

        // Now bind the server:
        server.listen(8000) { res: AsyncResult<HttpServer?> ->
            if (res.succeeded()) {
                startPromise.complete()
            } else {
                startPromise.fail(res.cause())
            }
        }
    }
}

private fun handleConnection(conn: Connection): Observable<Int> {
    val failed = false //模拟处理成功
    val firstPull = if (failed)
        Observable.error(Exception("failed")) else
        Observable.just(10000).doOnNext { registerFns(conn) } //注册功能
    val pulls = conn.msgs.map { 1 } //收1个拉1个
    return Observable.merge(pulls, firstPull)
        .onErrorComplete()
        .doOnEach(conn.msgPuller) //拉取连接级消息（非常重要）
}

private fun registerFns(conn: Connection) {
    Observable.merge(
        accFn(conn),  //其他fn....
        otherFn()
    ).onErrorComplete().subscribe()
}

private fun otherFn(): Observable<OnStream?>? {
    return Observable.empty()
}

//累加收到的数据个数
private fun accFn(conn: Connection): Observable<OnStream> =
    conn.register("acc").doOnNext { (header, bufs, bufPuller) ->
        //验证请求
        println("onHeader:${header.fn}")
        val df = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //处理上游发来的数据（未向上游拉取数据时是不会收到数据的）
        val handledBufs = bufs
//            .observeOn(Schedulers.computation())
//            .doOnNext { buf -> println("handle $buf on ${Thread.currentThread().name} : ${Thread.currentThread().id}") }
            .scan(0) { acc, _ -> acc + 1 }
            .map { acc ->
                val json: JsonObject = JsonObject()
                    .put("time", df.format(System.currentTimeMillis()))
                    .put("acc", acc)
                json.toBuffer().bytes
            }
        //创建下游流（无副作用，直到订阅其中的isSendable时才会发送header）
        val downStream = conn.stream(
            Header.newBuilder()
                .setFn(header.outputTo)
                .setBufType("json")
                .build(),
            handledBufs
        )
        //让拉取上游数据的速度与下游流的拉取速度相同
        downStream.pulls.subscribe(bufPuller)
        //副作用操作，会发送header
        downStream.isSendable.subscribe({}, {})
    }

private fun createWebSockets(options: HttpServerOptions): Observable<ServerWebSocket> =
    Observable.create { emitter ->
        val vertx = Vertx.vertx()
        val server = vertx.createHttpServer(options)
        server.webSocketHandler { ws: ServerWebSocket? ->
            println("onWs:" + Thread.currentThread().id)
            emitter.onNext(ws)
        }.listen(8000)
        println("listen on 8000")
    }

private fun parseToSocket(ws: ServerWebSocket): Single<Socket> =
    Single.create { emitter ->
        val sender = PublishSubject.create<ByteArray>()
        val theEnd = Observable.create<Any> { emitter1 ->
            ws.closeHandler {
//                    System.out.println("onErr close:");
                emitter1.onError(Exception(ws.closeReason()))
            }
        }
        val buffers = Observable.create<ByteArray> { emitter1 ->
            sender.takeUntil(theEnd).subscribe(
                { buf ->
//                            System.out.println("to send buf:"+ buf);
                    ws.write(Buffer.buffer(buf))
                },
                { err ->
                    if (!ws.isClosed) ws.close(4000.toShort(), err.message)
                    emitter1.tryOnError(java.lang.Exception(err.message))
                },
                {
                    ws.close()
                    emitter1.onComplete()
                }
            )
            ws.binaryMessageHandler { buffer ->
//                    System.out.println("onBuffer:"+Thread.currentThread().getId());
                emitter1.onNext(buffer.bytes)
//                emitter1.onNext(buffer.byteBuf.nioBuffer().array())
            }
            emitter1.setDisposable(Disposable.fromAction { sender.onComplete() })
        }.takeUntil(theEnd).share()
        emitter.onSuccess(Socket(buffers, sender))
    }
