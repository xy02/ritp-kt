package com.github.xy02.ritp_kt

import com.google.protobuf.ByteString
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import net.i2p.crypto.eddsa.EdDSAEngine
import net.i2p.crypto.eddsa.EdDSAPrivateKey
import net.i2p.crypto.eddsa.EdDSAPublicKey
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec
import ritp.Info
import ritp.InfoOrBuilder
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.security.MessageDigest
import java.security.Signature
import java.util.concurrent.atomic.AtomicInteger


fun init(myInfo: Info, sockets: Observable<Socket>, options: InitOptions = InitOptions()): Observable<Context> {
    val connections = initWith(myInfo)(sockets)
        .filter { conn ->
            if (conn.remoteInfo.appName.isNullOrEmpty() && !options.denyAnonymousApp) true
            else verifyInfo(conn.remoteInfo, options.appPublicKeyMap)
        }
        .publish().refCount(2)
    val getIdlestConnectionByAppName = getIdlestConnection(connections)
    return connections.map { conn -> Context(conn, getIdlestConnectionByAppName) }
}

fun nioSocketsSource(): SocketsSource {
    val selector = Selector.open()
    val sockets = Observable.create<SocketContext> {
        println("new NIO selector on ${Thread.currentThread().name} : ${Thread.currentThread().id}")
        while (selector.isOpen) {
            selector.select()
            val ite = selector.selectedKeys().iterator()
            while (ite.hasNext()) {
                val key = ite.next()
                ite.remove()
                if (!key.isValid) continue
                when (key.readyOps()) {
                    SelectionKey.OP_ACCEPT -> {
                        val ssc = key.channel() as ServerSocketChannel
                        val groupId = key.attachment() as Int
//                        println("OP_ACCEPT: localPort=${ssc.socket().localPort}, groupId=$groupId")
                        val theSocket = try {
                            val sc = ssc.accept()
//                            println("sc: port=${sc.socket().port}, localPort=${sc.socket().localPort}")
                            sc.configureBlocking(false)
                            val socket = newSocketFromSocketChannel(sc, key.selector())
                            Single.just(socket)
                        } catch (ex: Exception) {
                            Single.error(ex)
                        }
                        it.onNext(SocketContext(groupId, theSocket))
                    }
                    SelectionKey.OP_CONNECT -> {
                        val sc = key.channel() as SocketChannel
                        val groupId = key.attachment() as Int
//                        println("OP_CONNECT: localPort=${sc.socket().localPort}, groupId=$groupId")
                        val theSocket = try {
                            sc.finishConnect()
//                            println("sc: port=${sc.socket().port}, localPort=${sc.socket().localPort}")
                            val socket = newSocketFromSocketChannel(sc, key.selector())
                            Single.just(socket)
                        } catch (ex: Exception) {
                            println("finishConnect: $ex")
                            sc.close()
                            key.cancel()
                            Single.error(ex)
                        }
                        it.onNext(SocketContext(groupId, theSocket))
                    }
                    SelectionKey.OP_READ -> {
                        val sc = key.channel() as SocketChannel
                        val att = key.attachment() as SocketChannelAttachment
//                        println("sc: ${sc}, att:${att}")
                        var loop = true
                        do {
                            loop = readSocketChannel(sc, att)
                        } while (loop)
                    }
                }
            }
        }
//        println("selector closed tid : ${Thread.currentThread().id}")
        it.onComplete()
    }.subscribeOn(Schedulers.io())
    val getSocketContextByGroupId = getSubValues(sockets) { it.groupId }
    val gid = AtomicInteger()
    val newGroupId = { gid.getAndIncrement() }
    return SocketsSource(selector, newGroupId, getSocketContextByGroupId)
}

fun nioServerSockets(
    source: SocketsSource,
    options: TCPServerOptions = TCPServerOptions(),
): Observable<Socket> {
    val gid = source.newGroupId()
    return source.getSocketContextByGroupId(gid)
        .doOnSubscribe {
            val ssc = ServerSocketChannel.open()
            ssc.configureBlocking(false)
            ssc.register(source.selector, SelectionKey.OP_ACCEPT, gid)
            ssc.socket().bind(options.address)
        }
        .subscribeOn(Schedulers.io())
        .flatMapMaybe { cx ->
            cx.theSocket
                .doOnError { ex -> ex.printStackTrace() }
                .onErrorComplete()
        }
}

fun nioClientSocket(
    source: SocketsSource,
    address: SocketAddress,
): Single<Socket> {
    return Single.fromCallable { source.newGroupId() }
        .flatMap { gid ->
//            println("gid $gid")
            source.getSocketContextByGroupId(gid)
                .doOnSubscribe {
                    val sc = SocketChannel.open()
                    sc.configureBlocking(false)
                    sc.register(source.selector, SelectionKey.OP_CONNECT, gid)
                    sc.connect(address)
                    source.selector.wakeup()
//                    println("connect")
                }
                .subscribeOn(Schedulers.io())
                .take(1)
                .singleOrError()
                .flatMap { cx -> cx.theSocket }
        }
}

fun verifyInfo(info: Info, appPublicKeyMap: Map<String, ByteArray>): Boolean {
    val pk = appPublicKeyMap[info.appName] ?: return false
    val spec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519)
    val keySpec = EdDSAPublicKeySpec(pk, spec)
    val key = EdDSAPublicKey(keySpec)
    //Signature sgr = Signature.getInstance("EdDSA", "I2P");
    val sgr = EdDSAEngine(MessageDigest.getInstance(spec.hashAlgorithm))
    sgr.initVerify(key)
    updateSignature(sgr, info)
    return sgr.verify(info.sign.toByteArray())
}

fun signInfoByEd25519(info: Info.Builder, seed: ByteArray) {
    val spec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519)
    val keySpec = EdDSAPrivateKeySpec(seed, spec)
    val key = EdDSAPrivateKey(keySpec)
    val sgr = EdDSAEngine(MessageDigest.getInstance(spec.hashAlgorithm))
    sgr.initSign(key)
    updateSignature(sgr, info)
    val sig = sgr.sign()
    info.sign = ByteString.copyFrom(sig)
}

fun updateSignature(sgr: Signature, info: InfoOrBuilder) {
    sgr.update(info.version.toByteArray(Charsets.UTF_8))
    sgr.update(info.data.toByteArray())
    sgr.update(info.dataType.toByteArray())
    sgr.update(info.appName.toByteArray())
    sgr.update(ByteBuffer.allocate(4).putInt(info.signAlgValue))
    sgr.update(ByteBuffer.allocate(8).putLong(info.signAt))
}