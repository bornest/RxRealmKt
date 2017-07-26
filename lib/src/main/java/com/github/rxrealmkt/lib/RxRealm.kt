package com.github.rxrealmkt.lib

import android.content.Context
import android.support.annotation.Keep
import com.github.kotlinutils.concurrent.java.extensions.curThreadNameInBr
import com.github.kotlinutils.concurrent.java.extensions.v
import com.github.kotlinutils.rx2.extensions.loggingOnError
import com.github.kotlinutils.rx2.extensions.loggingOnSuccess
import com.github.unitimber.core.loggable.Loggable
import com.github.unitimber.core.loggable.extensions.i
import com.github.unitimber.core.loggable.extensions.v
import io.reactivex.Observable
import io.reactivex.Scheduler
import io.reactivex.Single
import io.reactivex.SingleEmitter
import io.realm.ObjectChangeSet
import io.realm.OrderedCollectionChangeSet
import io.realm.OrderedRealmCollectionChangeListener
import io.realm.Realm
import io.realm.RealmConfiguration
import io.realm.RealmObject
import io.realm.RealmObjectChangeListener
import io.realm.RealmQuery
import io.realm.RealmResults
import java.io.Closeable
import java.io.InvalidObjectException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.full.isSubclassOf


typealias RealmCollectionUpdates<T> = Pair<RealmResults<T>, OrderedCollectionChangeSet?>
typealias RealmObjectUpdates<T> = Pair<T, ObjectChangeSet?>


/**
 * Class that provides a certain level of abstraction from Realm Database implementation
 * in a form of more simple get/put/delete methods
 *
 * @param context Application context
 *
 * @author Borislav Nesterov
 * @since 2016 09 09 - created
 */
class RxRealm(
    context: Context,
    override var loggingEnabled: Boolean = true,
    override val logTag: String = "RxRealm",
    configParams: (RealmConfiguration.Builder.() -> RealmConfiguration.Builder) = { apply{} }
) : Loggable {
    private val realmConfig: RealmConfiguration
    
    init {
        Realm.init(context)

        realmConfig = RealmConfiguration.Builder()
            .configParams()
            .build()

        Realm.setDefaultConfiguration(realmConfig)
    }
    
    //FIXME: fix all *WithoutUpdates() methods (add closable logic)
    
    //FIXME: fix the transaction logic in every method (throw exceptions instead of multiple loggingOnError() calls)
    
    //region GET
        /**
         * Get all objects of the given class from Realm DB as an Observable, perform it on a thread specified by given looper.
         *
         * It will emit the current RealmResults when subscribed to.
         * RealmResults will continually be emitted as the RealmResults are updated - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param T class of requested objects
         * @param scheduler looper of the thread, on which you want to perform the query and return the results
         *
         * @return [Observable]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
         */
        inline fun <reified T> getAllWithUpdates(scheduler: Scheduler): Observable<RealmCollectionUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getAllWithUpdates(T::class.java, scheduler)
        }
    
    
        /**
         * Get all objects of the given class from Realm DB as an Observable, perform it on a thread specified by given looper.
         *
         * It will emit the current RealmResults when subscribed to.
         * RealmResults will continually be emitted as the RealmResults are updated - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param clazz class of requested objects
         * @param scheduler looper of the thread, on which you want to perform the query and return the results
         *
         * @return [Observable]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
         */
        @Keep
        fun <T> getAllWithUpdates(clazz: Class<T>, scheduler: Scheduler
        ): Observable<RealmCollectionUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getWithUpdates(clazz, scheduler, "ALL") {this}
        }
        
    
        /**
         * Get all objects of the given class from Realm DB that match the given query as an Observable, perform it on a thread specified by given looper.
         *
         * It will emit the current RealmResults when subscribed to.
         * RealmResults will continually be emitted as the RealmResults are updated - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param T class of requested objects
         * @param scheduler scheduler, on which you want to perform the query and return the results
         * @param query RealmQuery parameters in a form of lambda
         *
         * @return [Observable]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
         */
        inline fun <reified T> getWithUpdates(scheduler: Scheduler,
                                              queryDescription: String = "Q",
                                              crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Observable<RealmCollectionUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getWithUpdates(T::class.java, scheduler, queryDescription, query)
        }
    
    
        /**
         * Get all objects of the given class from Realm DB that match the given query as an Observable, perform it on a thread specified by given looper.
         *
         * It will emit the current RealmResults when subscribed to.
         * RealmResults will continually be emitted as the RealmResults are updated - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param clazz class of requested objects
         * @param scheduler scheduler, on which you want to perform the query and return the results
         * @param query RealmQuery parameters in a form of lambda
         *
         * @return [Observable]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
         */
        inline fun <T> getWithUpdates(clazz: Class<T>,
                                      scheduler: Scheduler,
                                      queryDescription: String = "Q",
                                      crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Observable<RealmCollectionUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            val changeListenerRef: AtomicReference<OrderedRealmCollectionChangeListener<RealmResults<T>>> = AtomicReference()
            val realmResRef: AtomicReference<RealmResults<T>> = AtomicReference()
            val opName = "getWithUpdates(${clazz.simpleName}, $queryDescription)"
            return Observable.using(
                {
                    try {
                        Realm.getDefaultInstance().also {
                            i { "$curThreadNameInBr $opName opened Realm" }
                        }
                    } catch (t: Throwable) {
                        throw Throwable("$opName failed: $t")
                    }
                },
                {
                    realm ->
                    Observable.create<RealmCollectionUpdates<T>> {
                        emitter ->
                        try {
                            val changeListener = OrderedRealmCollectionChangeListener<RealmResults<T>> {
                                realmResults, orderedCollectionChangeSet ->
                                val collectionUpdatesStr = (realmResults to orderedCollectionChangeSet).str
                                v { "$curThreadNameInBr Change listener received $collectionUpdatesStr" }
                                if (!emitter.isDisposed) {
                                    v { "$curThreadNameInBr Change listener emmitting $collectionUpdatesStr" }
                                    emitter.onNext(realmResults to orderedCollectionChangeSet)
                                }
                            }
                            
                            val realmRes = realm.where(clazz)
                                .query()
                                .findAllAsync()
                                .apply { addChangeListener(changeListener) }
                    
                            realmResRef.v = realmRes
                            changeListenerRef.v = changeListener
                        } catch (t: Throwable) {
                            loggingOnError(emitter) { "$opName failed: $t" }
                        }
                    }
                },
                {
                    realm ->
                    realmResRef.v.removeChangeListener(changeListenerRef.v)
                    realm.close()
                    i { "$curThreadNameInBr $opName closed Realm" }
                }
            )
                .subscribeOn(scheduler)
                .unsubscribeOn(scheduler)
        }
    
    
        inline fun <reified T> getFirstWithUpdates(scheduler: Scheduler,
                                                   crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Observable<RealmObjectUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getFirstWithUpdates(T::class.java, scheduler, query)
        }
    
    
        inline fun <T> getFirstWithUpdates(clazz: Class<T>,
                                           scheduler: Scheduler,
                                           crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Observable<RealmObjectUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            val changeListenerRef: AtomicReference<RealmObjectChangeListener<T>> = AtomicReference()
            val realmObjRef: AtomicReference<T> = AtomicReference()
            val opName = "getFirstWithUpdates(${clazz.simpleName})"
            return Observable.using(
                {
                    try {
                        Realm.getDefaultInstance().also {
                            i { "$curThreadNameInBr $opName opened Realm" }
                        }
                    } catch (t: Throwable) {
                        throw Throwable("$opName failed: $t")
                    }
                },
                {
                    realm ->
                    Observable.create<RealmObjectUpdates<T>> {
                        emitter ->
                        try {
                            val changeListener = RealmObjectChangeListener<T> {
                                realmObject, objectChangeSet ->
                                val objectUpdatesStr = (realmObject to objectChangeSet).str
                                v { "$curThreadNameInBr Change listener received $objectUpdatesStr" }
                                if (!emitter.isDisposed) {
                                    v { "$curThreadNameInBr Change listener emmitting $objectUpdatesStr" }
                                    emitter.onNext(realmObject to objectChangeSet)
                                }
                            }
                    
                            val realmObj : T = realm.where(clazz)
                                .query()
                                .findFirstAsync()
                                .apply { addChangeListener(changeListener) }
                            
                            realmObjRef.v = realmObj
                            changeListenerRef.v = changeListener
                        } catch (t: Throwable) {
                            loggingOnError(emitter) { "$opName failed: $t" }
                        }
                    }
                },
                {
                    realm ->
                    realmObjRef.v.removeChangeListener(changeListenerRef.v)
                    realm.close()
                    i { "$curThreadNameInBr $opName closed Realm" }
                }
            )
                .subscribeOn(scheduler)
                .unsubscribeOn(scheduler)
        }
    
    
//        /**
//         * Get all objects of the given class from Realm DB that match the given query as a Single, perform it on a thread specified by given scheduler.
//         *
//         * It will emit the current RealmResults when subscribed to.
//         *
//         * **Scheduler**:
//         * getWithoutUpdates() does not operate by default on a particular [Scheduler].
//         *
//         *
//         * @param T class of requested objects
//         * @param query RealmQuery parameters in a form of lambda
//         *
//         * @return [Single]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
//         */
//        inline fun <reified T> getWithoutUpdates(crossinline query: RealmQuery<T>.() -> RealmQuery<T>): Single<RealmResults<T>>
//            where T : RealmObject, T : RealmDbEntity<T> {
//            return getWithoutUpdates(T::class.java, query)
//        }
//
//
//        /**
//         * Get all objects of the given class from Realm DB that match the given query as a Single,
//         * perform it on a thread specified by given scheduler.
//         *
//         * It will emit the current RealmResults when subscribed to.
//         *
//         * **Scheduler**:
//         * getWithoutUpdates() does not operate by default on a particular [Scheduler].
//         *
//         *
//         * @param clazz class of requested objects
//         * @param query RealmQuery parameters in a form of lambda
//         *
//         * @return [Single]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
//         */
//        inline fun <T> getWithoutUpdates(clazz: Class<T>,
//                                         crossinline query: RealmQuery<T>.() -> RealmQuery<T>
//        ): Single<RealmResults<T>>
//            where T : RealmObject, T : RealmDbEntity<T> {
//            val opName = "getWithoutUpdates(${clazz.simpleName})"
//            //FIXME: closes Realm right after getting data making this data unusable
//            return realmSingle(opName) {
//                realm ->
//                Single.create<RealmResults<T>> {
//                    emitter ->
//                    try {
//                        val realmRes = realm.where(clazz).query().findAll()
//                        loggingOnSuccess(emitter, realmRes) { "$opName success: got ${realmRes.size} items from DB" }
//                    } catch (t: Throwable) {
//                        loggingOnError(emitter) { "$opName failed: $t" }
//                    }
//                }
//            }
//        }
    
        /**
         * Get all objects of the given class from Realm DB that match the given query as a Single.
         *
         * It will perform the query when subscribed to.
         *
         *
         * *Note: You have to manually close the emitted [Closeable] when you no longer need the results.
         * Realm instance will **leak** otherwise.*
         *
         *
         * **Scheduler**:
         * getWithoutUpdates() does not operate by default on a particular [Scheduler].
         *
         *
         * @param T class of requested objects
         * @param query RealmQuery parameters in a form of lambda
         *
         * @return [Single]<[Pair]<[RealmResults<* : RealmObject>][RealmResults], [Closeable]>>, where * is determined by T.
         */
        inline fun <reified T> getWithoutUpdates(crossinline query: RealmQuery<T>.() -> RealmQuery<T>): Single<Pair<RealmResults<T>, Closeable>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getWithoutUpdates(T::class.java, query)
        }
    
    
        /**
         * Get all objects of the given class from Realm DB that match the given query as a Single.
         *
         * It will perform the query when subscribed to.
         *
         *
         * *Note: You have to manually close the emitted [Closeable] when you no longer need the results.
         * Realm instance will **leak** otherwise.*
         *
         *
         * **Scheduler**:
         * getWithoutUpdates() does not operate by default on a particular [Scheduler].
         *
         *
         * @param clazz class of requested objects
         * @param query RealmQuery parameters in a form of lambda
         *
         * @return [Single]<[Pair]<[RealmResults<* : RealmObject>][RealmResults], [Closeable]>>, where * is determined by clazz.
         */
        inline fun <T> getWithoutUpdates(clazz: Class<T>,
                                         crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Single<Pair<RealmResults<T>, Closeable>>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "getWithoutUpdates(${clazz.simpleName})"
            return Single.create<Pair<RealmResults<T>, Closeable>> {
                emitter ->
                val realm = try {
                    Realm.getDefaultInstance().also {
                        i { "$curThreadNameInBr $opName opened Realm" }
                    }
                } catch (t: Throwable) {
                    loggingOnError(emitter, t) { "$opName failed: $t" }
                    null
                }
                
                if (realm != null) {
                    emitter.setCancellable(realm::close)
                    val realmRes = realm.where(clazz).query().findAll()
                    emitter.onSuccess(realmRes to object : Closeable {
                        private val isClosed = AtomicBoolean(false)
                        override fun close() {
                            if (isClosed.compareAndSet(false, true)) realm.close()
                        }
                    })
                }
            }
        }
    
        /**
         * Get first object of the given class from Realm DB that match the given query as a Single,
         * perform it on a thread specified by given scheduler.
         *
         * It will emit the retrieved object when subscribed to.
         *
         * **Scheduler**:
         * getFirstWithoutUpdates() does not operate by default on a particular [Scheduler].
         *
         *
         * @param T class of requested object
         * @param query RealmQuery parameters in a form of lambda
         *
         * @return [Single]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
         */
        inline fun <reified T> getFirstWithoutUpdates(crossinline query: RealmQuery<T>.() -> RealmQuery<T>): Single<T>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getFirstWithoutUpdates(T::class.java, query)
        }
    
        /**
         * Get first object of the given class from Realm DB that match the given query as a Single, perform it on a thread specified by given scheduler.
         *
         * It will emit the retrieved object when subscribed to.
         *
         * **Scheduler**:
         * getFirstWithoutUpdates() does not operate by default on a particular [Scheduler].
         *
         *
         * @param clazz class of requested object
         * @param query RealmQuery parameters in a form of lambda
         *
         * @return [Single]<[RealmResults<* : RealmObject>][RealmResults]>, where * is determined by clazz.
         */
        inline fun <T> getFirstWithoutUpdates(clazz: Class<T>,
                                              crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Single<T>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "getFirstWithoutUpdates(${clazz.simpleName})"
            return realmSingle(opName) {
                realm ->
                Single.create<T> {
                    emitter ->
                    try {
                        realm.where(clazz).query().findFirst()
                            ?.let {
                                loggingOnSuccess(emitter, it) { "$opName success: got ${it.strIdentifier} from DB" }
                            } ?:
                                loggingOnError(emitter, EmptyQueryResultException("$opName failed: no object matched the query"))
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    
        /**
         * Get an unmanaged copy of all objects of the given class from Realm DB that match the given query, return result as an Single.
         *
         * It will emit the current RealmResults when subscribed to.
         *
         * **Scheduler**:
         * getUnmanagedCopy() does not operate by default on a particular [Scheduler].
         *
         *
         * @param T class of requested objects
         * @param query [RealmQuery] parameters in a form of lambda
         *
         * @return [Single]<[List<* : RealmObject>][List]>, where * is determined by clazz.
         */
        inline fun <reified T> getUnmanagedCopy(crossinline query: RealmQuery<T>.() -> RealmQuery<T>): Single<List<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getUnmanagedCopy(T::class.java, query)
        }
    
    
        /**
         * Get an unmanaged copy of all objects of the given class from Realm DB that match the given query as an Single, perform it on a thread specified by given scheduler.
         *
         * It will emit the current RealmResults when subscribed to.
         *
         * **Scheduler**:
         * getUnmanagedCopy() does not operate by default on a particular [Scheduler].
         *
         *
         * @param clazz class of requested objects
         * @param query RealmQuery parameters in a form of lambda
         *
         * @return [Single]<[List<* : RealmObject>][List]>, where * is determined by clazz.
         */
        inline fun <T> getUnmanagedCopy(clazz: Class<T>,
                                        crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Single<List<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "getUnmanagedCopy(${clazz.simpleName})"
            return realmSingle(opName) {
                realm ->
                Single.create<List<T>> {
                    emitter ->
                    try {
                        val snapshot = realm.copyFromRealm( realm.where(clazz).query().findAll() )
                        loggingOnSuccess(emitter, snapshot) { "$opName success: got ${snapshot.size} items from DB" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    //endregion


    //region PUT
        /**
         * Put the provided object into Realm DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * put() does not operate by default on a particular [Scheduler].
         *
         * @param item object to put in Realm DB, has to extend RealmObject
         *
         * @return [Single]<[String]> that, when subscribed to, performs the put operation and emits the result (success or error with description of what caused it).
         */
        fun <T> put(item: T): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "put(${item.strIdentifier})"
            return realmSingle(opName) {
                realm ->
                Single.create<String> {
                    emitter ->
                    try {
                        realm.executeTransaction {
                            realm.copyToRealmOrUpdate(item)
                        }
                        loggingOnSuccess(emitter) { "$opName successful" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    
        /**
         * Put the provided object into Realm DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * put() does not operate by default on a particular [Scheduler].
         *
         * @param items a [Collection]<[T]> of items to be put into Realm
         *
         * @return [Single]<[String]> that, when subscribed to, performs the put operation and emits the result (success or error with description of what caused it).
         */
        fun <T> put(items: Collection<T>): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            if (items.isEmpty()) {
                return Single.error<String>(IllegalArgumentException("put() failed: the collection is empty!"))
            }
            
            val opName = "put(${items.strIds})"
            
            return realmSingle(opName) {
                realm ->
                Single.create<String> {
                    emitter ->
                    try {
                        realm.executeTransaction {
                            realm.copyToRealmOrUpdate(items)
                        }
                        loggingOnSuccess(emitter) { "$opName successful" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    //endregion


    //region DELETE
        /**
         * Delete all objects of the given class from Realm DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * deleteAll() does not operate by default on a particular [Scheduler].         *
         *
         * @param T class of objects to delete
         *
         * @return [Single]<[String]> that, when subscribed to, performs the delete operation and emits the result (success or error with description of what caused it).
         */
        inline fun <reified T> deleteAll(): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return deleteAll(T::class.java)
        }
    
        /**
         * Delete all objects of the given class from Realm DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * deleteAll() does not operate by default on a particular [Scheduler].         *
         *
         * @param clazz class of objects to delete
         *
         * @return [Single]<[String]> that, when subscribed to, performs the delete operation and emits the result (success or error with description of what caused it).
         */
        fun <T> deleteAll(clazz: Class<T>): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "deleteAll(${clazz.simpleName})"
            return realmSingle(opName) {
                realm ->
                Single.create<String> {
                    emitter ->
                    try {
                        realm.executeTransaction {
                            realm.where(clazz).findAll().apply {
                                if (clazz.kotlin.isSubclassOf(RealmComplexObject::class))
                                    forEach { (it as RealmComplexObject).deleteCascade() }
                                else
                                    deleteAllFromRealm()
                            }
                        }
                        loggingOnSuccess(emitter) { "$opName successful" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    
    
        /**
         * Delete all objects of the given class from Realm DB that match the given query and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * deleteAll() does not operate by default on a particular [Scheduler].
         *
         * @param T class of objects to delete
         * @param query [RealmQuery] parameters in a form of lambda
         *
         * @return [Single]<[String]> that, when subscribed to, performs the delete operation and emits the result (success or error with description of what caused it).
         */
        inline fun <reified T> delete(crossinline query: RealmQuery<T>.() -> RealmQuery<T>): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return delete(T::class.java, query)
        }
    
        /**
         * Delete all objects of the given class from Realm DB that match the given query and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * deleteAll() does not operate by default on a particular [Scheduler].
         *
         * @param clazz class of objects to delete
         * @param query [RealmQuery] parameters in a form of lambda
         *
         * @return [Single]<[String]> that, when subscribed to, performs the delete operation and emits the result (success or error with description of what caused it).
         */
        inline fun <T> delete(clazz: Class<T>,
                              crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "delete(${clazz.simpleName})"
            return realmSingle(opName) {
                realm ->
                Single.create<String> {
                    emitter ->
                    try {
                        realm.executeTransaction {
                            realm.where(clazz).query().findAll().apply {
                                if (clazz.kotlin.isSubclassOf(RealmComplexObject::class))
                                    forEach { (it as RealmComplexObject).deleteCascade() }
                                else
                                    deleteAllFromRealm()
                            }
                        }
                        loggingOnSuccess(emitter) { "$opName successful" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    //endregion


    //region UPDATE
        inline fun <reified T> update(item: T,
                                      crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return update(T::class.java, item, changes)
        }
    
    
        inline fun <T> update(clazz: Class<T>,
                              item: T,
                              crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            val pkName = item.primaryKeyName
            val pkValue = item.primaryKeyValue
            val opName = "update(${item.strIdentifier})"
            return realmSingle(opName) {
                realm ->
                Single.create<String> {
                    emitter ->
                    try {
                        realm.executeTransaction {
                            val realmRes = realm.where(clazz).equalTo(pkName, pkValue).findAll()
                            if (realmRes.isEmpty()) {
                                loggingOnError(emitter) { "$opName failed: No such object in DB!" }
                            } else {
                                val obj = realmRes[0]
                                if (!obj.isValid) {
                                    loggingOnError(emitter) { "$opName failed: found object is no longer valid!" }
                                } else {
                                    obj.changes()
                                }
                            }
                        }
                
                        loggingOnSuccess(emitter) { "$opName successful" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    
    
        inline fun <reified T> updateAll(crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                         crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return updateAll(T::class.java, query, changes)
        }
    
    
        inline fun <T> updateAll(clazz: Class<T>,
                                 crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                 crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "updateAll(${clazz.simpleName})"
            return realmSingle(opName) {
                realm ->
                Single.create<String> {
                    emitter ->
                    try {
                        realm.executeTransaction {
                            val realmResults = realm.where(clazz).query().findAll()
                            
                            if (realmResults.isEmpty()) {
                                throw EmptyQueryResultException("$opName failed: No such object in DB!")
                            } else {
                                realmResults
                                    .filter { it.isValid }
                                    .takeIf { it.isNotEmpty() }
                                        ?.forEach { it.changes() }
                                        ?: throw InvalidObjectException("$opName failed: found objects are no longer valid!")
                            }
                        }
                        loggingOnSuccess(emitter) { "$opName successful" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter, t)
                    }
                }
            }
        }
    
    
        inline fun <reified T> updateFirst(crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                           crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return updateFirst(T::class.java, query, changes)
        }
    
    
        inline fun <T> updateFirst(clazz: Class<T>,
                                   crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                   crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "updateFirst(${clazz.simpleName})"
            return realmSingle("updateFirst()") {
                realm ->
                Single.create<String> {
                    emitter ->
                    try {
                        var success = true
            
                        realm.executeTransaction {
                            realm.where(clazz).query().findFirst()
                                ?.apply {
                                    changes()
                                } ?:
                                loggingOnError(emitter, EmptyQueryResultException("updateFirst() failed: No such object in DB!")).let { success = false }
                
                        }
            
                        if (success) loggingOnSuccess(emitter) { "updateFirst() successful" }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "updateFirst() failed: $t" }
                    }
                }
            }
        }
    
        inline fun <reified T> updateFirstAndReturnUnmanagedCopy(crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                                                 crossinline changes: T.() -> Unit
        ): Single<T>
            where T : RealmObject, T : RealmDbEntity<T> {
            return updateFirstAndReturnUnmanagedCopy(T::class.java, query, changes)
        }
    
        inline fun <T> updateFirstAndReturnUnmanagedCopy(clazz: Class<T>,
                                                         crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                                         crossinline changes: T.() -> Unit
        ): Single<T>
            where T : RealmObject, T : RealmDbEntity<T> {
            val opName = "updateFirstAndReturnUnmanagedCopy(${clazz.simpleName})"
            return realmSingle(opName) {
                realm ->
                Single.create<T> {
                    emitter ->
                    try {
                        var successfullyUpdated = true
                        var objIdColumn : String = ""
                        var objId : String = ""
            
                        realm.executeTransaction {
                            realm.where(clazz).query().findFirst()
                                ?.apply {
                                    changes()
                                    objIdColumn = this@apply.primaryKeyName
                                    objId = this@apply.primaryKeyValue
                                } ?:
                                loggingOnError(emitter, EmptyQueryResultException("$opName failed: (update) No such object in DB!")).let { successfullyUpdated = false }
                        }
            
                        if (successfullyUpdated) {
                            //TODO: eliminate race condition by making async
                            realm.where(clazz).equalTo(objIdColumn, objId).findFirst()
                                ?.let {
                                    loggingOnSuccess(emitter, realm.copyFromRealm(it)) { "$opName successful" }
                                } ?:
                                loggingOnError(emitter, EmptyQueryResultException("$opName failed: (return) No such object in DB!"))
                        }
                    } catch (t: Throwable) {
                        loggingOnError(emitter) { "$opName failed: $t" }
                    }
                }
            }
        }
    //endregion
    
    
    //region Transaction
        inline fun transaction(crossinline actions: (Realm, AtomicBoolean) -> Unit): Single<String> {
            return realmTransactionSingle("transaction()") {
                realm, emitter ->
                try {
                    val success = AtomicBoolean(true)
                    
                    realm.executeTransaction {
                        actions(realm, success)
                    }
                    
                    if (success.get()) loggingOnSuccess(emitter) { "$curThreadNameInBr transaction() successful" }
                    else loggingOnError(emitter) { "$curThreadNameInBr transaction() failed!" }
                } catch (t: Throwable) {
                    loggingOnError(emitter) { "$curThreadNameInBr transaction() failed: $t" }
                }
            }
        }
    //endregion

    
    //region Util
        inline fun <T> realmSingle(operationName: String, crossinline singleFactory: (Realm) -> Single<T>): Single<T> {
            return Single.using(
                {
                    try {
                        Realm.getDefaultInstance().also {
                            i { "$curThreadNameInBr $operationName opened Realm" }
                        }
                    } catch (t: Throwable) {
                        throw Throwable("$operationName failed: $t")
                    }
                },
                {
                    realm ->
                    singleFactory(realm)
                },
                {
                    realm ->
                    realm.close()
                    i { "$curThreadNameInBr $operationName closed Realm." }
                }
            )
        }

        inline fun <T> realmTransactionSingle(operationName: String,
                                              crossinline singleBody: (Realm, SingleEmitter<T>) -> Unit
        ): Single<T> {
            return Single.using(
                {
                    try {
                        Realm.getDefaultInstance().also {
                            i { "$curThreadNameInBr $operationName opened Realm" }
                        }
                    } catch (t: Throwable) {
                        throw Throwable("$operationName failed: $t")
                    }
                },
                {
                    realm ->
                    Single.create<T> {
                        emitter ->
                        singleBody(realm, emitter)
                    }
                },
                {
                    realm ->
                    realm.close()
                    i { "$curThreadNameInBr $operationName closed Realm." }
                }
            )
        }
    //endregion
    
    class EmptyQueryResultException : Exception {
        constructor() : super()
        constructor(message: String) : super(message)
        constructor(cause: Throwable) : super(cause)
        constructor(message: String, cause: Throwable) : super(message, cause)
    }
}

//region Extensions
    inline val OrderedCollectionChangeSet.str: String
        get() = with(StringBuilder(8))
        {
            append("\n{")
            append("\n\tDeletions: ")
            append(this@str.deletions.toList().toString())
            append("\n\tInsertions: ")
            append(this@str.insertions.toList().toString())
            append("\n\tModifications: ")
            append(this@str.changes.toList().toString())
            append("\n}")
            toString()
        }

    inline val <T> RealmCollectionUpdates<T>.str: String
        where T : RealmDbEntity<T>, T : RealmObject
        @JvmName("getRealmCollectionUpdatesStr")
        get() {
            return "\nRealmResults: ${this.first.strIds}\nUpdates: " + (this.second?.str ?: "{}")
        }

    inline val ObjectChangeSet.str: String
        get() = with(StringBuilder()){
            append("\n{")
            if (this@str.isDeleted) {
                append("\n\tisDeleted: true")
            } else if (this@str.changedFields.isNotEmpty()) {
                append("\n\tChanged Fields: ")
                append(this@str.changedFields.toList().toString())
            } else {
                append("\n\tNothing Changed")
            }
            append("\n}")
            toString()
        }

    inline val <T> RealmObjectUpdates<T>.str: String
        where T : RealmDbEntity<T>, T : RealmObject
        @JvmName("getRealmObjectUpdatesStr")
        get() = "\nRealmObject: ${this.first.strIdentifier}\nUpdates: " + (this.second?.str ?: "{}")
//endregion