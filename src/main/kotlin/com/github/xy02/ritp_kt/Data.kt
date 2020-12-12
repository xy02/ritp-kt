package com.github.xy02.ritp_kt

import io.reactivex.rxjava3.core.*
import ritp.Frame
import ritp.Header
import ritp.Info
import ritp.Msg
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.Selector

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
    val msgSender: Observer<Msg>,
    val msgPuller: Observer<Int>,
    val pullFramesToSend: Observable<Frame>,
    val msgFramesToSend: Observable<Frame>,
    val newStreamId: () -> Int,
    val sendableMsgsAmounts: Observable<Int>,
)

data class Stream(
    val pulls: Observable<Int>,
    val isSendable: Observable<Boolean>,
    val bufSender: Observer<ByteArray>,
)

data class OnStream(
    val header: Header,
    val bufs: Observable<ByteArray>,
    val bufPuller: Observer<Int>
)

data class Socket(
    val buffers: Observable<ByteArray>,
    val sender: Observer<ByteArray>,
)

data class Connection(
    val remoteInfo: Info,
    val msgs: Observable<Msg>,
    val msgPuller: Observer<Int>,
    val register: (String) -> Observable<OnStream>,
    val stream: (Header) -> Stream,
    val sendableMsgsAmounts: Observable<Int>,
)

data class Context(
    val connection: Connection,
    //按应用名获取最空闲度的连接
    val getIdlestConnectionByAppName: (appName: String) -> Maybe<Connection>,
)

data class InitOptions(
    val denyAnonymousApp: Boolean = false,
    val appPublicKeyMap: Map<String, ByteArray> = mapOf(),
)

data class TCPServerOptions(
    val address: SocketAddress = InetSocketAddress(8001),
)

data class SocketsSource(
    val selector: Selector,
    val newGroupId: () -> Int,
    val getSocketContextByGroupId: (Int) -> Observable<SocketContext>,
)

data class SocketContext(
    val groupId: Int,
    val theSocket: Single<Socket>,
)

internal data class SocketChannelAttachment(
    val emitter: ObservableEmitter<ByteArray>,
    val bufOfLength: ByteBuffer = ByteBuffer.allocate(4).put(0),
    var bufOfBody: ByteBuffer? = null,
)

internal data class WindowState(val windowSize: Int, val increment: Int, val decrement: Int)
