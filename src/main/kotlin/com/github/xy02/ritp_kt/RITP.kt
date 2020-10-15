package com.github.xy02.ritp_kt

import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.BehaviorSubject
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.Subject
import ritp.*
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

private fun getGroupedInput(buffers: Observable<ByteArray>): GroupedInput {
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

private fun getPullIncrements(
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
    ).subscribe({ }, emitter::onError, emitter::onComplete)
    emitter.setDisposable(Disposable.fromAction { sub.dispose() })
}

private fun getOutputContext(msgs: Observable<Msg>, pullsToGetMsg: Observable<Int>): OutputContext {
    val outputQueue: Queue<Msg> = ConcurrentLinkedQueue()
    val msgsFromQueue = pullsToGetMsg
        .flatMap { pull ->
            Observable.fromIterable(
                outputQueue
            ).take(pull.toLong())
        }
        .share()
    val msgSender = PublishSubject.create<Msg>()
    val sendNotifier = BehaviorSubject.createDefault(0)
    val sendableMsgsAmounts = Observable.merge(sendNotifier, pullsToGetMsg, msgsFromQueue.map { -1 })
        .scan(0, Integer::sum).replay(1).refCount()
    val msgFramesToSend = Observable.merge(
        msgsFromQueue,
        msgSender.withLatestFrom(sendableMsgsAmounts,
            { msg, amount ->
                if (amount > 0) Observable.just(msg) else Observable.empty<Msg>()
                    .doOnComplete { outputQueue.add(msg) }
            }
        ).flatMap { x -> x }.doOnNext { sendNotifier.onNext(-1) }
    ).map { msg ->
        Frame.newBuilder().setMsg(msg).build()
    }
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
    return OutputContext(msgSender, msgPuller, pullFramesToSend, msgFramesToSend, newStreamId)
}

private fun getInputContext(msgs: Observable<Msg>): InputContext {
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

private fun registerWith(
    msgSender: Subject<Msg>,
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
        pullMsgsToSend.subscribe(msgSender) //side effect
        OnStream(header, bufs, bufPuller)
    }
}

private fun streamWith(
    newStreamId: () -> Int, msgSender: Subject<Msg>,
    getCloseMsgsByStreamId: (Int) -> Observable<Msg>,
    getPullMsgsByStreamId: (Int) -> Observable<Msg>
): (Header, Observable<ByteArray>) -> Stream = { header, bufs ->
    val streamId = newStreamId()
    val sendable = BehaviorSubject.createDefault(false)
    val msgs = bufs.withLatestFrom(sendable,
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
        .doOnEach(msgSender)
        .share()
    val theEndOfMsgs = msgs.lastOrError().toObservable()
    val remoteClose = getCloseMsgsByStreamId(streamId)
        .take(1)
        .flatMap { msg ->
            Observable.error<Any>(Exception(msg.close.message))
        }
    val pulls = getPullMsgsByStreamId(streamId)
        .map { msg -> msg.pull }
        .takeUntil(theEndOfMsgs)
        .takeUntil(remoteClose)
        .share()
    val sendableAmounts = Observable.merge(msgs.map { -1 }, pulls)
        .scan(0, Integer::sum)
//        .replay(1).refCount()
    val isSendable = sendableAmounts.map { amount -> amount > 0 }
        .distinctUntilChanged()
        .doOnEach(sendable)
        .doOnSubscribe {
            val msg = Msg.newBuilder().setHeader(header).setStreamId(streamId).build()
            msgSender.onNext(msg)
        }
        .share()
    Stream(pulls, isSendable)
}

fun initWith(myInfo: Info): (Observable<Socket>) -> Observable<Connection> = { sockets ->
    sockets.flatMapSingle { socket ->
        val (info, msgs, pullsToGetMsg, remoteClose) = getGroupedInput(socket.buffers)
        val (msgSender, msgPuller, pullFramesToSend, msgFramesToSend, newStreamId)
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
                Connection(remoteInfo, msgs, msgPuller, register, stream)
            }
    }
}