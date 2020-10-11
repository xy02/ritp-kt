package com.github.xy02.ritp_kt

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.ObservableEmitter
import io.reactivex.rxjava3.disposables.Disposable
import java.util.*
import java.util.concurrent.ConcurrentHashMap

fun <K, V> getSubValues(values: Observable<V>, keySelector: (V) -> K): (K) -> Observable<V> {
    val m = ConcurrentHashMap<K, MutableSet<ObservableEmitter<V>>>()
//    val m = mutableMapOf<K,MutableSet<ObservableEmitter<V>>>()
    val theEnd = values
        .doOnNext { v: V ->
            val key = keySelector(v)
            m[key]?.forEach { emitter -> emitter.onNext(v) }
        }
        .ignoreElements()
        .cache()
    return { key ->
        Observable.create { emitter ->
            val emitterSet = m.getOrPut(key) {
                Collections.newSetFromMap(ConcurrentHashMap())
//                mutableSetOf()
            }
            emitterSet.add(emitter)
            val d = theEnd.subscribe(
                { emitter.onComplete() }
            ) { e -> emitter.onError(e) }
            emitter.setDisposable(Disposable.fromAction {
                emitterSet.remove(emitter)
                if (emitterSet.size == 0) {
                    m.remove(key)
                }
                d.dispose()
            })
        }
    }
}