package com.github.rxrealmkt.lib

/**
 * Realm DB objects that contains references to other Realm DB objects
 */
interface RealmComplexObject {
    /**
     * Perform a cascade deletion of this object
     *
     * Deletes this object from DB along with every object that is referenced from it
     */
    fun deleteCascade()
}