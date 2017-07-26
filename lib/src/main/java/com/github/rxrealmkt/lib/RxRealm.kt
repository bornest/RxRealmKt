package com.github.rxrealmkt.lib

import android.content.Context
import android.os.HandlerThread
import android.support.annotation.Keep
import com.github.kotlinutils.concurrent.java.extensions.curThreadNameInBr
import com.github.kotlinutils.concurrent.java.extensions.v
import com.github.kotlinutils.rx2.extensions.loggingOnError
import com.github.kotlinutils.rx2.extensions.loggingOnSuccess
import com.github.kotlinutils.rx2.extensions.scheduler
import com.github.unitimber.core.loggable.Loggable
import com.github.unitimber.core.loggable.extensions.i
import com.github.unitimber.core.loggable.extensions.v
import io.reactivex.Observable
import io.reactivex.Scheduler
import io.reactivex.Single
import io.reactivex.SingleEmitter
import io.reactivex.android.schedulers.AndroidSchedulers
import io.realm.ObjectChangeSet
import io.realm.OrderedCollectionChangeSet
import io.realm.OrderedRealmCollectionChangeListener
import io.realm.Realm
import io.realm.RealmConfiguration
import io.realm.RealmObject
import io.realm.RealmObjectChangeListener
import io.realm.RealmQuery
import io.realm.RealmResults
import io.realm.annotations.PrimaryKey
import java.io.Closeable
import java.io.InvalidObjectException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.full.isSubclassOf

/**
 * RealmResults paired with optional OrderedCollectionChangeSet
 */
typealias RealmCollectionUpdates<T> = Pair<RealmResults<T>, OrderedCollectionChangeSet?>

/**
 * RealmObject paired with optional ObjectChangeSet
 */
typealias RealmObjectUpdates<T> = Pair<T, ObjectChangeSet?>


/**
 * Class that provides a certain level of abstraction from Realm Database implementation
 * in a form of more simple get/put/delete methods
 *
 * @param context Application context
 * @param loggingEnabled set to false to disable logging (**true by default**)
 * @param configParams [RealmConfiguration.Builder] params in a from of lambda with receiver
 *
 * @author Borislav Nesterov
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
        
    //region GET
        /**
         * Get all objects of the given class from Realm DB with consecutive updates
         *
         * Returned [Observable] will emit [RealmCollectionUpdates] when subscribed to.
         * RealmCollectionUpdates will continually be emitted in response to DB updates - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param T class of requested objects
         * @param scheduler [Scheduler] on which you want to perform the query and return the results (**underlying thread needs to have a looper!**)
         *
         * @return [Observable]<[RealmCollectionUpdates< T >][RealmCollectionUpdates]> operating on specified scheduler
         */
        inline fun <reified T> getAllWithUpdates(scheduler: Scheduler): Observable<RealmCollectionUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getAllWithUpdates(T::class.java, scheduler)
        }
    
    
        /**
         * Get all objects of the given class from Realm DB with consecutive updates
         *
         * Returned [Observable] will emit [RealmCollectionUpdates] when subscribed to.
         * RealmCollectionUpdates will continually be emitted in response to DB updates - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param clazz - [Class] of requested objects
         * @param scheduler [Scheduler]: scheduler on which you want to perform the query and return the results (**underlying thread needs to have a looper!**)
         *
         * @return [Observable]<[RealmCollectionUpdates<T>][RealmCollectionUpdates]> operating on specified scheduler
         */
        @Keep
        fun <T> getAllWithUpdates(clazz: Class<T>, scheduler: Scheduler
        ): Observable<RealmCollectionUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getWithUpdates(clazz, scheduler, "ALL") {this}
        }
    
    
        /**
         * Get all objects of the given class that match the given query from Realm DB with consecutive updates
         *
         * Returned [Observable] will emit [RealmCollectionUpdates] when subscribed to.
         * RealmCollectionUpdates will continually be emitted in response to DB updates - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param T class of requested objects
         * @param scheduler [Scheduler] on which you want to perform the query and return the results (**underlying thread needs to have a looper!**)
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         *
         * @return [Observable]<[RealmCollectionUpdates< T >][RealmCollectionUpdates]> operating on specified scheduler
         */
        inline fun <reified T> getWithUpdates(scheduler: Scheduler,
                                              queryDescription: String = "Q",
                                              crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Observable<RealmCollectionUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getWithUpdates(T::class.java, scheduler, queryDescription, query)
        }
    
    
        /**
         * Get all objects of the given class that match the given query from Realm DB with consecutive updates
         *
         * Returned [Observable] will emit [RealmCollectionUpdates] when subscribed to.
         * RealmCollectionUpdates will continually be emitted in response to DB updates - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param @param clazz - [Class] of requested objects
         * @param scheduler [Scheduler] on which you want to perform the query and return the results (**underlying thread needs to have a looper!**)
         * @param queryDescription - (optional query description)
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         *
         * @return [Observable]<[RealmCollectionUpdates< T >][RealmCollectionUpdates]> operating on specified scheduler
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
    
        /**
         * Get first object of the given class that matches the given query from Realm DB with consecutive updates
         *
         * It will emit [RealmObjectUpdates] when subscribed to.
         * RealmObjectUpdates will continually be emitted in response to DB updates - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param T class of requested object
         * @param scheduler [Scheduler] on which you want to perform the query and return the results (**underlying thread needs to have a looper!**)
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         *
         * @return [Observable]<[RealmObjectUpdates< T >][RealmObjectUpdates]> operating on specified scheduler
         */
        inline fun <reified T> getFirstWithUpdates(scheduler: Scheduler,
                                                   crossinline query: RealmQuery<T>.() -> RealmQuery<T>
        ): Observable<RealmObjectUpdates<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getFirstWithUpdates(T::class.java, scheduler, query)
        }
    
    
        /**
         * Get first object of the given class that matches the given query from Realm DB with consecutive updates
         *
         * Returned [Observable] will emit [RealmObjectUpdates] when subscribed to.
         * RealmObjectUpdates will continually be emitted in response to DB updates - onComplete will never be called.
         * If you want to get result only once, use take(1) operator.
         * If you would like it to stop emitting items, unsubscribe.
         *
         * @param clazz [Class] of requested object
         * @param scheduler [Scheduler] on which you want to perform the query and return the results (**underlying thread needs to have a looper!**)
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         *
         * @return [Observable]<[RealmObjectUpdates< T >][RealmObjectUpdates]> operating on specified scheduler
         */
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
    
    
        /**
         * Get an unmanaged copy of all objects of the given class from Realm DB that match the given query
         *
         * Returned [Single] will emit the current RealmResults when subscribed to.
         *
         * **Scheduler**:
         * getUnmanagedCopy() does not operate by default on a particular [Scheduler].
         *
         * @param T class of requested objects
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         *
         * @return [Single]<[List< T >][List]>, where * is determined by clazz.
         */
        inline fun <reified T> getUnmanagedCopy(crossinline query: RealmQuery<T>.() -> RealmQuery<T>): Single<List<T>>
            where T : RealmObject, T : RealmDbEntity<T> {
            return getUnmanagedCopy(T::class.java, query)
        }
    
    
        /**
         * Get an unmanaged copy of all objects of the given class from Realm DB that match the given query
         *
         * Returned [Single] will emit the current RealmResults when subscribed to.
         *
         * **Scheduler**:
         * getUnmanagedCopy() does not operate by default on a particular [Scheduler].
         *
         * @param clazz [Class] of requested objects
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         *
         * @return [Single]<[List< T >][List]>, where * is determined by clazz.
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
         * Put object into Realm DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         *
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
         * Put collection of objects into Realm DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         *
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
         *
         * deleteAll() does not operate by default on a particular [Scheduler].
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
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
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
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
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
        /**
         * Update item in DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * update() does not operate by default on a particular [Scheduler].
         *
         * @param T item class
         * @param item item to update in DB (can be an unmanaged copy)
         * @param changes changes to perform on the item
         *
         * @return [Single]<[String]> that, when subscribed to, performs the update operation and emits the result (success or error with description of what caused it).
         */
        inline fun <reified T> update(item: T, crossinline changes: T.() -> Unit): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return update(T::class.java, item, changes)
        }
    
        /**
         * Update item in DB and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * update() does not operate by default on a particular [Scheduler].
         *
         * @param T item [Class]
         * @param item item to update in DB (can be an unmanaged copy)
         * @param changes changes to perform on the item
         *
         * @return [Single]<[String]> that, when subscribed to, performs the update operation and emits the result (success or error with description of what caused it).
         */
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
    
        /**
         * Update all items in DB that match the query and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * updateAll() does not operate by default on a particular [Scheduler].
         *
         * @param T item class
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         * @param changes changes to perform on the item
         *
         * @return [Single]<[String]> that, when subscribed to, performs the update operation and emits the result (success or error with description of what caused it).
         */
        inline fun <reified T> updateAll(crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                         crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return updateAll(T::class.java, query, changes)
        }
    
        /**
         * Update all items in DB that match the query and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * updateAll() does not operate by default on a particular [Scheduler].
         *
         * @param clazz item [Class]
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         * @param changes changes to perform on the item
         *
         * @return [Single]<[String]> that, when subscribed to, performs the update operation and emits the result (success or error with description of what caused it).
         */
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
    
        /**
         * Update the first item in DB that matches the query and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * updateFirst() does not operate by default on a particular [Scheduler].
         *
         * @param T item class
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         * @param changes changes to perform on the item
         *
         * @return [Single]<[String]> that, when subscribed to, performs the update operation and emits the result (success or error with description of what caused it).
         */
        inline fun <reified T> updateFirst(crossinline query: RealmQuery<T>.() -> RealmQuery<T>,
                                           crossinline changes: T.() -> Unit
        ): Single<String>
            where T : RealmObject, T : RealmDbEntity<T> {
            return updateFirst(T::class.java, query, changes)
        }
    
        /**
         * Update the first item in DB that match the query and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * updateFirst() does not operate by default on a particular [Scheduler].
         *
         * @param clazz item [Class]
         * @param query [RealmQuery] parameters (in a form of lambda with receiver)
         * @param changes changes to perform on the item
         *
         * @return [Single]<[String]> that, when subscribed to, performs the update operation and emits the result (success or error with description of what caused it).
         */
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
    //endregion
    
    
    //region Transaction
        /**
         * Perform a DB transaction and return result - all wrapped in a [Single].
         *
         * **Scheduler**:
         * transaction() does not operate by default on a particular [Scheduler].
         *
         * @param actions actions to be performed by transaction in a form of lambda with 2 parameters: [Realm] - DB reference, [AtomicBoolean] - "success" flag (true by default)
         *
         * @return [Single]<[String]> that, when subscribed to, performs the transaction and emits the result (success or error with description of what caused it).
         */
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
        /**
         * Wrap a [Single] in another Single that automatically opens and closes Realm (**for internal use**)
         *
         * @param operationName name of the operation that is performed by wrapped Single
         * @param singleFactory lambda that takes a [Realm] and returns a [Single]
         *
         * @return wrapped [Single]
         */
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
    
        /**
         * Wrap a [Single] in another Single that automatically opens and closes Realm (**for internal use**)
         *
         * @param operationName name of the operation that is performed by wrapped Single
         * @param singleBody lambda that takes a [Realm] & [SingleEmitter] and returns a [Single]
         *
         * @return wrapped [Single]
         */
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
    
    /**
     * Exception that is thrown on empty query result
     */
    class EmptyQueryResultException : Exception {
        constructor() : super()
        constructor(message: String) : super(message)
        constructor(cause: Throwable) : super(cause)
        constructor(message: String, cause: Throwable) : super(message, cause)
    }
}

//region Extensions
    /**
     * Custom [String] representation of [OrderedCollectionChangeSet]
     */
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

    /**
     * Custom [String] representation of [RealmCollectionUpdates]
     */
    inline val <T> RealmCollectionUpdates<T>.str: String
        where T : RealmDbEntity<T>, T : RealmObject
        @JvmName("getRealmCollectionUpdatesStr")
        get() {
            return "\nRealmResults: ${this.first.strIds}\nUpdates: " + (this.second?.str ?: "{}")
        }

    /**
     * Custom [String] representation of [ObjectChangeSet]
     */
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

    /**
     * Custom [String] representation of [RealmObjectUpdates]
     */
    inline val <T> RealmObjectUpdates<T>.str: String
        where T : RealmDbEntity<T>, T : RealmObject
        @JvmName("getRealmObjectUpdatesStr")
        get() = "\nRealmObject: ${this.first.strIdentifier}\nUpdates: " + (this.second?.str ?: "{}")
//endregion
