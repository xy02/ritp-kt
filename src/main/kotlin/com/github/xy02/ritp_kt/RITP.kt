package com.github.xy02.ritp_kt

import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Observer
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.BehaviorSubject
import io.reactivex.rxjava3.subjects.PublishSubject
import ritp.*
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger


internal fun getGroupedInput(buffers: Observable<ByteArray>): GroupedInput {
    val frames = buffers.map(Frame::parseFrom).share()
    val getFramesByType = getSubValues(frames) { frame -> frame.typeCase }
    val remoteClose = getFramesByType(Frame.TypeCase.CLOSE)
        .take(1).flatMap {
            Observable.error<Any>(
                Exception("close")
            )
        }.cache()
    val info = getFramesByType(Frame.TypeCase.INFO)
        .map { frame -> frame.info }.take(1).singleOrError()
    val pullsToGetMsg = getFramesByType(Frame.TypeCase.PULL)
        .map { frame -> frame.pull }
    val msgs = getFramesByType(Frame.TypeCase.MSG)
        .map { frame -> frame.msg }
    return GroupedInput(info, msgs, pullsToGetMsg, remoteClose)
}

internal fun getPullIncrements(
    msgs: Observable<Any>,
    pulls: Observable<Int>,
    overflowErr: Exception
): Observable<Int> = Observable.create { emitter ->
    val sub = Observable.merge(
        msgs.map { -1 }, pulls
    ).scan(
        WindowState(0, 0, 0),
        { preState, num ->
            var (windowSize, increment, decrement) = preState
            if (num > 0) increment += num else decrement -= num
            if (decrement > windowSize) throw overflowErr
            if (decrement >= windowSize / 2) {
                windowSize = windowSize - decrement + increment
                if (increment > 0) emitter.onNext(increment)
                increment = 0
                decrement = 0
            }
            WindowState(windowSize, increment, decrement)
        }
    ).subscribe({ }, emitter::tryOnError, emitter::onComplete)
    emitter.setDisposable(Disposable.fromAction { sub.dispose() })
}

internal fun getOutputContext(msgs: Observable<Msg>, pullsToGetMsg: Observable<Int>): OutputContext {
    val sendNotifier = BehaviorSubject.createDefault(0)
    val outputQueue: Queue<Msg> = ConcurrentLinkedQueue()
    val msgsFromQueue = pullsToGetMsg
        .doOnComplete(sendNotifier::onComplete)
        .doOnError(sendNotifier::onError)
        .flatMap { pull ->
            //fix bug
            Observable
                .generate<Msg> { emitter ->
                    val v = outputQueue.poll()
                    if (v != null) emitter.onNext(v)
                    else emitter.onComplete()
                }
                .take(pull.toLong())
        }
    val msgSender = PublishSubject.create<Msg>()
    val sendableMsgsAmounts = Observable.merge(sendNotifier, pullsToGetMsg)
        .scan(0, { a, b -> a + b }).replay(1).refCount()
    val msgFramesToSend = Observable
        .merge(
            msgsFromQueue,
            msgSender.withLatestFrom(sendableMsgsAmounts,
                { msg, amount ->
                    if (amount > 0) Observable.just(msg) else Observable.empty<Msg>()
                        .doOnComplete { outputQueue.add(msg) }
                }
            ).flatMap { x -> x }
        )
        .map { msg ->
            Frame.newBuilder().setMsg(msg).build()
        }
        .doOnNext { sendNotifier.onNext(-1) }
    val msgPuller = PublishSubject.create<Int>()
    val pullIncrements = getPullIncrements(
        msgs.cast(
            Any::class.java
        ), msgPuller, ProtocolException("input msg overflow")
    )
    val pullFramesToSend = pullIncrements.map { pull ->
        Frame.newBuilder().setPull(pull).build()
    }
    val sid = AtomicInteger()
    val newStreamId = { sid.getAndIncrement() }
    return OutputContext(msgSender, msgPuller, pullFramesToSend, msgFramesToSend, newStreamId, sendableMsgsAmounts)
}

internal fun getInputContext(msgs: Observable<Msg>): InputContext {
    val getMsgsByType = getSubValues(msgs, Msg::getTypeCase)
    val closeMsgs = getMsgsByType(Msg.TypeCase.CLOSE)
    val getCloseMsgsByStreamId = getSubValues(closeMsgs, Msg::getStreamId)
    val pullMsgs = getMsgsByType(Msg.TypeCase.PULL)
    val getPullMsgsByStreamId = getSubValues(pullMsgs, Msg::getStreamId)
    val endMsgs = getMsgsByType(Msg.TypeCase.END)
    val getEndMsgsByStreamId = getSubValues(endMsgs, Msg::getStreamId)
    val bufMsgs = getMsgsByType(Msg.TypeCase.BUF)
    val getBufMsgsByStreamId = getSubValues(bufMsgs, Msg::getStreamId)
    val headerMsgs = getMsgsByType(Msg.TypeCase.HEADER)
    val getHeaderMsgsByFn = getSubValues(headerMsgs) { msg -> msg.header.fn }
    return InputContext(
        getHeaderMsgsByFn,
        getCloseMsgsByStreamId,
        getPullMsgsByStreamId,
        getEndMsgsByStreamId,
        getBufMsgsByStreamId
    )
}

internal fun registerWith(
    msgSender: Observer<Msg>,
    getHeaderMsgsByFn: (String) -> Observable<Msg>,
    getEndMsgsByStreamId: (Int) -> Observable<Msg>,
    getBufMsgsByStreamId: (Int) -> Observable<Msg>
): (String) -> Observable<OnStream> = { fn ->
    getHeaderMsgsByFn(fn).map { msg ->
        val streamId = msg.streamId
        val header = msg.header
        val theEnd = getEndMsgsByStreamId(streamId)
            .take(1)
            .flatMap { m ->
                if (m.end.reason != End.Reason.COMPLETE) Observable.error(Exception("cancel"))
                else Observable.just(m.end)
            }
        val bufPuller = PublishSubject.create<Int>()
        val bufs = getBufMsgsByStreamId(streamId)
            .map { m -> m.buf.toByteArray() }
            .takeUntil(theEnd)
            .takeUntil(bufPuller.ignoreElements().toObservable<Any>())
            .share()
        val pullIncrements = getPullIncrements(
            bufs.cast(
                Any::class.java
            ), bufPuller, ProtocolException("input buf overflow")
        )
        val pullMsgsToSend = pullIncrements
            .map { pull -> Msg.newBuilder().setPull(pull) }
            .concatWith(
                Observable.just(
                    Msg.newBuilder()
                        .setClose(Close.newBuilder().setReason(Close.Reason.APPLICATION_ERROR).setMessage(""))
                )
            )
            .onErrorReturn { err ->
                val message = err.message ?: ""
                Msg.newBuilder()
                    .setClose(Close.newBuilder().setReason(Close.Reason.APPLICATION_ERROR).setMessage(message))
            }
            .map { builder -> builder.setStreamId(streamId).build() }
            .doOnNext { msgSender.onNext(it) }
        pullMsgsToSend.subscribe() //side effect
        OnStream(header, bufs, bufPuller)
    }
}

internal fun streamWith(
    newStreamId: () -> Int, msgSender: Observer<Msg>,
    getCloseMsgsByStreamId: (Int) -> Observable<Msg>,
    getPullMsgsByStreamId: (Int) -> Observable<Msg>
): (Header) -> Stream = { header ->
    val streamId = newStreamId()
    val sendingMsg = PublishSubject.create<Msg>()
    val theEndOfMsgs = sendingMsg.ignoreElements().toObservable<Msg>()
    val remoteClose = getCloseMsgsByStreamId(streamId)
        .take(1)
        .flatMap { msg ->
            Observable.error<Any>(Exception(msg.close.message))
        }
    val pulls = getPullMsgsByStreamId(streamId)
        .map { msg -> msg.pull }
        .takeUntil(theEndOfMsgs)
        .takeUntil(remoteClose)
        .replay(1)
        .refCount()
    val sendableAmounts = Observable.merge(sendingMsg.map { -1 }, pulls)
        .scan(0, { a, b -> a + b })
    val isSendable = sendableAmounts.map { amount -> amount > 0 }
        .distinctUntilChanged()
        .doOnSubscribe {
            val msg = Msg.newBuilder().setHeader(header).setStreamId(streamId).build()
            msgSender.onNext(msg)
        }
    val bufSender = PublishSubject.create<ByteArray>()
    //side effect
    bufSender.withLatestFrom(isSendable,
        { buf, ok ->
            if (ok) Observable.just(Msg.newBuilder().setBuf(ByteString.copyFrom(buf)))
            else Observable.empty()
        })
        .flatMap { o -> o }
        .concatWith(
            Observable.just(
                Msg.newBuilder().setEnd(End.newBuilder().setReason(End.Reason.COMPLETE))
            )
        )
        .onErrorReturn { err ->
            val message = err.message ?: ""
            Msg.newBuilder().setEnd(End.newBuilder().setReason(End.Reason.CANCEL).setMessage(message))
        }
        .map { builder -> builder.setStreamId(streamId).build() }
        .doOnNext { msgSender.onNext(it) }
        .subscribe(sendingMsg)
    Stream(pulls, isSendable, bufSender)
}

internal fun initWith(myInfo: Info): (Observable<Socket>) -> Observable<Connection> = { sockets ->
    sockets.flatMapMaybe { socket ->
        val (info, msgs, pullsToGetMsg, remoteClose) = getGroupedInput(socket.buffers)
        val (msgSender, msgPuller, pullFramesToSend, msgFramesToSend, newStreamId, sendableMsgsAmounts)
                = getOutputContext(msgs, pullsToGetMsg)
        val (getHeaderMsgsByFn, getCloseMsgsByStreamId, getPullMsgsByStreamId, getEndMsgsByStreamId, getBufMsgsByStreamId)
                = getInputContext(msgs)
        val sendingBytes = Observable.merge(msgFramesToSend, pullFramesToSend)
            .startWithItem(
                Frame.newBuilder().setInfo(myInfo).build()
            )
            .onErrorReturn { err ->
                if (err is ProtocolException) Frame.newBuilder().setClose(
                    Close.newBuilder()
                        .setReason(Close.Reason.PROTOCOL_ERROR)
                        .setMessage(err.message)
                ).build()
                else Frame.newBuilder().setClose(
                    Close.newBuilder()
                        .setReason(Close.Reason.APPLICATION_ERROR)
                        .setMessage(err.message)
                ).build()
            }
            .takeUntil(remoteClose)
            .map { frame -> frame.toByteArray() }
        info.doOnSubscribe { sendingBytes.subscribe(socket.sender) }
            .map { remoteInfo ->
                val register = registerWith(msgSender, getHeaderMsgsByFn, getEndMsgsByStreamId, getBufMsgsByStreamId)
                val stream = streamWith(newStreamId, msgSender, getCloseMsgsByStreamId, getPullMsgsByStreamId)
                Connection(remoteInfo, msgs, msgPuller, register, stream, sendableMsgsAmounts)
            }
            .onErrorComplete()
    }
}

internal fun createContexts(conns: Observable<Connection>): Observable<Context> {
    val map = ConcurrentHashMap<String, Connection>()
    val connections = Observable.just(conns)
        .doOnNext {
            it.groupBy { conn -> conn.remoteInfo.appName }
                .flatMap { group ->
                    group
                        .flatMap { conn ->
                            conn.sendableMsgsAmounts.map { amount -> Pair(amount, conn) }
                                .onErrorComplete()
                                .doOnComplete { if (map[group.key] == conn) map.remove(group.key) }
                        }
                        .distinctUntilChanged { (amount, conn), (oldAmount, oldConn) -> conn != oldConn && amount > oldAmount }
                        .doOnNext { (_, conn) -> map[group.key] = conn }
                }
                .subscribe()//side effect
        }
        .flatMap { it }
    //按应用名获取最空闲度的连接
    val getIdlestConnectionByAppName = { appName: String ->
        Maybe.create<Connection> {
            val conn = map[appName]
            if (conn == null) it.onComplete() else it.onSuccess(conn)
        }
    }
    return connections.map { connection ->
        Context(connection, getIdlestConnectionByAppName)
    }.share()
}

internal fun newSocketFromSocketChannel(sc: SocketChannel, selector: Selector): Socket {
    val sender = PublishSubject.create<ByteArray>()
    val buffers = Observable.create<ByteArray> { emitter1 ->
        val d = sender
//            .observeOn(Schedulers.io())
            .subscribe(
                { buf ->
//                            System.out.println("to send buf:"+ buf);
                    try {
                        val size = buf.size
                        val lengthBuf = ByteBuffer.allocate(3)
                            .put((size shr 16 and 0xff).toByte())
                            .put((size shr 8 and 0xff).toByte())
                            .put((size and 0xff).toByte())
                        lengthBuf.flip()
                        while (lengthBuf.hasRemaining()) sc.write(lengthBuf)
                        val bodyBuf = ByteBuffer.wrap(buf)
                        while (bodyBuf.hasRemaining()) sc.write(bodyBuf)
//                    println("write $bodyBuf on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
                    } catch (e: Exception) {
                        println("write SocketChannel: $e")
                        sc.close()
                    }
                },
                { err ->
                    println("sender onError:$err")
                    sc.close()
                },
                {
                    println("sender onComplete")
                    sc.close()
                }
            )
        sc.register(selector, SelectionKey.OP_READ, SocketChannelAttachment(emitter1))
        emitter1.setDisposable(Disposable.fromAction { d.dispose() })
    }
//        .observeOn(Schedulers.computation())
        .share()
    return Socket(buffers, sender)
}

internal fun readSocketChannel(sc: SocketChannel, att: SocketChannelAttachment): Boolean {
    val buf = att.bufOfBody ?: att.bufOfLength
    try {
        val bytesRead = sc.read(buf)
        if (bytesRead == -1) {
            sc.close()
            if (!att.emitter.isDisposed) att.emitter.tryOnError(java.nio.channels.ClosedChannelException())
            return false
        }
    } catch (e: Exception) {
        println("read SocketChannel: $e")
        if (!att.emitter.isDisposed) att.emitter.tryOnError(e)
    }
    if (buf.hasRemaining()) return false
    if (att.bufOfBody == null) {
        //从bufOfLength获取body长度
        buf.flip()
        att.bufOfBody = ByteBuffer.allocate(buf.int)
        buf.clear()
        buf.put(0)//body长度实际只有3字节，这里把最高位字节设置为0
    } else {
//        println("read $buf on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
        att.emitter.onNext(buf.array())
        att.bufOfBody = null
    }
//    readSocketChannel(sc, att)
    return true
}
