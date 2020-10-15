package com.github.xy02.ritp_kt

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.subjects.Subject
import ritp.Frame
import ritp.Header
import ritp.Info
import ritp.Msg

data class GroupedInput(
    val info: Single<Info>,
    val msgs: Observable<Msg>,
    val pullsToGetMsg: Observable<Int>,
    val remoteClose: Observable<Any>
)

data class InputContext(
    val getHeaderMsgsByFn: (String) -> Observable<Msg>,
    val getCloseMsgsByStreamId: (Int) -> Observable<Msg>,
    val getPullMsgsByStreamId: (Int) -> Observable<Msg>,
    val getEndMsgsByStreamId: (Int) -> Observable<Msg>,
    val getBufMsgsByStreamId: (Int) -> Observable<Msg>
)

data class OutputContext(
    val msgSender: Subject<Msg>,
    val msgPuller: Subject<Int>,
    val pullFramesToSend: Observable<Frame>,
    val msgFramesToSend: Observable<Frame>,
    val newStreamId: () -> Int
)

data class Stream(
    val pulls: Observable<Int>,
    val isSendable: Observable<Boolean>,
)

data class OnStream(
    val header: Header,
    val bufs: Observable<ByteArray>,
    val bufPuller: Subject<Int>
)

data class Socket(
    val buffers: Observable<ByteArray>,
    val sender: Subject<ByteArray>
)

data class Connection(
    val remoteInfo: Info,
    val msgs: Observable<Msg>,
    val msgPuller: Subject<Int>,
    val register: (String) -> Observable<OnStream>,
    val stream: (Header, Observable<ByteArray>) -> Stream
)

internal data class WindowState(val windowSize: Int, val increment: Int, val decrement: Int)
