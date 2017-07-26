@file:Suppress("NOTHING_TO_INLINE")

package com.github.rxrealmkt.lib

import com.github.kotlinutils.core.extensions.simpleNameOrEmptyStr
import io.realm.RealmList
import io.realm.RealmModel

/**
 * Created by nbv54 on 10-Mar-17.
 */
interface RealmDbEntity<out T : RealmModel> {
    val primaryKeyName: String
    val primaryKeyValue: String
    val strIdentifier: String
        get() = "${this::class.simpleNameOrEmptyStr}#$primaryKeyValue"
    
    fun deepCopy(): T
}

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

inline fun <T> RealmList<T>.deepCopy(): RealmList<T>
    where T : RealmDbEntity<T>, T : RealmModel {
    val resList = RealmList<T>()
    
    this.forEach { resList.add(it.deepCopy()) }
    
    return resList
}