@file:Suppress("NOTHING_TO_INLINE")

package com.github.rxrealmkt.lib

import com.github.kotlinutils.core.extensions.simpleNameOrEmptyStr
import io.realm.RealmList
import io.realm.RealmModel

/**
 * Realm DB entity that exposes info about its primary key and supports deep copy creation
 */
interface RealmDbEntity<out T : RealmModel> {
    /**
     * Name of primary key "column"
     */
    val primaryKeyName: String
    
    /**
     * Value of primary key
     */
    val primaryKeyValue: String
    
    /**
     * A combination of class name and PK value that uniquely identifies this entity
     */
    val strIdentifier: String
        get() = "${this::class.simpleNameOrEmptyStr}#$primaryKeyValue"
    
    /**
     * Create a deep copy
     *
     * @return a deep copy of this entity
     */
    fun deepCopy(): T
}

/**
 * Custom string representation of [RealmDbEntity.strIdentifier]s of this entity collection
 */
inline val <T> Collection<T>.strIds: String
    where T : RealmDbEntity<*>
    get() {
        if (this.isEmpty())
            return "[]"
        else
            return StringBuilder(this.size * 2 + 1).let {
                strBuilder ->
                strBuilder.append("\n[")
                this.forEach { strBuilder.append("\n\t").append(it.strIdentifier) }
                strBuilder.append("\n]")
                strBuilder.toString()
            }
    }

/**
 * Create a deep copy
 *
 * @return a deep copy of this [RealmList]
 */
inline fun <T> RealmList<T>.deepCopy(): RealmList<T>
    where T : RealmDbEntity<T>, T : RealmModel {
    val resList = RealmList<T>()
    
    this.forEach { resList.add(it.deepCopy()) }
    
    return resList
}