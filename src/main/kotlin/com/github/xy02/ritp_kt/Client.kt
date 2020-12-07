package com.github.xy02.ritp_kt

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.PublishSubject
import ritp.Header
import ritp.Info
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    val source = nioSocketsSource()
    val socket = nioClientSocket(source, InetSocketAddress("localhost", 8001))
    val socket2 = nioClientSocket(source, InetSocketAddress("localhost", 8001))
    val myInfo = Info.newBuilder().setVersion("0.2").build()
    val cxs = init(myInfo, Observable.merge(socket.toObservable(), socket2.toObservable())
        .retryWhen {
            it.flatMap { e ->
                println(e)
                Observable.timer(3, TimeUnit.SECONDS)
            }
        }
    )
    cxs
        .subscribeOn(Schedulers.newThread())
        .doOnComplete { println("cx onComplete") }
        .flatMap { cx -> handleConnection(cx.connection) }
        .subscribe(
            {},
            { err -> err.printStackTrace() },
        )
    Thread.sleep(1000000000)
}

private fun handleConnection(conn: Connection): Observable<Int> {
    println("remoteInfo.version: ${conn.remoteInfo.version}")
    val failed = false //模拟处理成功
    val firstPull = if (failed)
        Observable.error(Exception("failed")) else
        Observable.just(200000).doOnNext { registerFns(conn) } //注册功能
    val pulls = conn.msgs.map { 1 } //收1个拉1个
    return Observable.merge(pulls, firstPull)
        .onErrorComplete()
        .doOnEach(conn.msgPuller) //拉取连接级消息（非常重要）
}

private fun registerFns(conn: Connection) {
    Observable.merge(
//        accReplyFn(conn),  //其他fn....
//        accFn(conn),
        crazyAccReply(conn),
        crazyAcc(conn),
    ).onErrorComplete().subscribe()
}

private fun accReplyFn(conn: Connection): Observable<OnStream> =
    conn.register("accReply").doOnNext { (header, bufs, bufPuller) ->
        //验证请求
        println("onHeader:${header.fn}")
        val pulls = bufs.doOnNext { println(String(it)) }.map { 1 }
            .doOnComplete { println("bufs doOnComplete") }
            .doOnError { println("bufs doOnError, e:$it") }
        Observable.merge(pulls, Observable.just(1))
            .subscribe(bufPuller)
    }

private fun accFn(conn: Connection): Observable<Boolean> {
    val bufs = Observable.interval(2, TimeUnit.SECONDS)
        .map { ByteArray(2) }
    val stream =
        conn.stream(Header.newBuilder().setFn("acc").setOutputTo("accReply").build(), bufs)
    return stream.isSendable
        .doOnComplete { println("isSendable doOnComplete") }
        .doOnError { println("isSendable doOnError, e:$it") }
}

private fun crazyAcc(conn: Connection): Observable<Boolean> {
    val bufSender = PublishSubject.create<ByteArray>()
    val (pulls, isSendable) = conn.stream(
        Header.newBuilder().setFn("acc").setOutputTo("crazyAccReply").build(), bufSender
    )
    return isSendable.doOnSubscribe {
        Observable.timer(1, TimeUnit.SECONDS)
            .flatMap {
                pulls.flatMap { pull ->
//                        println("the pull is $pull")
                    Observable.just(ByteArray(1))
                        .repeat(pull.toLong())
                }
            }
            .subscribe(bufSender)
    }
}

private fun crazyAccReply(conn: Connection): Observable<OnStream> =
    conn.register("crazyAccReply").doOnNext { (header, bufs, bufPuller) ->
        var count = 0
        val d = Observable.interval(1, TimeUnit.SECONDS)
            .subscribe {
                println("${count / (it + 1)}/s")
            }
        bufs.scan(0) { acc, v -> acc + 1 }
            .doOnNext { count = it }
            .subscribe({}, { d.dispose() })
//            val begin = System.currentTimeMillis()

        //验证请求
        println("onHeader:${header.fn}")
        val pulls = bufs
//                .doOnNext { println("buf") }
            .map { 1 }
        Observable.merge(pulls, Observable.just(100000))
            .subscribe(bufPuller)
    }