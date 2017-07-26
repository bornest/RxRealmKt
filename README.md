# RxRealmKt

Android library built on top of Realm.

Provides seamless RxJava 2 integration and simple get/put/update/delete methods tailored for more idiomatic usage in Kotlin.

**Most of the functions and methods are `inline` and don’t add to your app’s method
count.**

## Setup

To use the latest SNAPSHOT add following code to your Gradle dependencies:

`compile 'com.github.bornest:RxRealmKt:-SNAPSHOT'`

## Usage

### Init

Create RxRealm singleton object (via Dagger 2 module or custom singleton class).

Dagger 2 module example:
```kotlin

@Module
class DbModule{
    @Provides
    @Singleton
    fun provideRealmDB(context : Context) : RxRealm {
        return RxRealm(context) {
            //RealmConfiguration builder parameters
            //...
            deleteRealmIfMigrationNeeded() //example builder parameter
        }
    }
}

```

Make your DB entities implement `RealmDbEntity` interface (at the moment it's required for proper functioning of some methods):

```kotlin
open class FirebaseTokenDbItem(
    @PrimaryKey open var userEmail: String = "",
    open var currentToken: String = "",
    open var lastSyncedToken: String = ""
) : RealmObject(), RealmDbEntity<FirebaseTokenDbItem>
{
    override val primaryKeyName: String
        get() = userEmailColumn
    override val primaryKeyValue: String
        get() = userEmail

    companion object {
        const val userEmailColumn = "userEmail"
        const val currentTokenColumn = "currentToken"
        const val lastSyncedTokenColumn = "lastSyncedToken"
    }

    override fun deepCopy(): FirebaseTokenDbItem {
        return FirebaseTokenDbItem(
            this.userEmail,
            this.currentToken,
            this.lastSyncedToken
        )
    }
}
```

### GET

Here are some of the methods for getting data from Realm DB.

(see all methods in [source](https://github.com/bornest/RxRealmKt/blob/master/lib/src/main/java/com/github/rxrealmkt/lib/RxRealm.kt) or javadoc)

#### getAllWithUpdates()
Get all objects of the given class from Realm DB with consecutive updates (as Observable).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - Scheduler with thread that has a looper (e.g. AndroidSchedulers.mainThread())

db.getAllWithUpdates<UserDbItem>(scheduler)
    .doOnNext {
        (users, changes) -> //users: RealmResults<UserDbItem>, changes: OrderedCollectionChangeSet?

        // do something
        // ...
    }
    .subscribe()
```

#### getWithUpdates()
Get all objects of the given class that match the given query from Realm DB with consecutive updates (as Observable).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - Scheduler with thread that has a looper (e.g. AndroidSchedulers.mainThread())

db.getWithUpdates<UserDbItem>(scheduler) {
    equalTo(UserDbItem.cityColumn, "New York")
    greaterThan(UserDbItem.ageColumn, 18)
}
    .doOnNext {
        (users, changes) -> //users: RealmResults<UserDbItem>, changes: OrderedCollectionChangeSet?

        //...
    }
    .subscribe()
```

#### getUnmanagedCopy()

Get an unmanaged copy of all objects of the given class from Realm DB that match the given query (as Single).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - any Scheduler (e.g. Schedulers.io())

db.getUnmanagedCopy<UserDbItem> {
    equalTo(UserDbItem.cityColumn, "New York")
    greaterThan(UserDbItem.ageColumn, 18)
}
    .doOnSuccess {
        users -> // List<UserDbItem>

        //...
    }
    .subscribeOn(scheduler)
    .subscribe()
```

### PUT

Here are some of the methods for putting data in Realm DB.

(see all methods in [source](https://github.com/bornest/RxRealmKt/blob/master/lib/src/main/java/com/github/rxrealmkt/lib/RxRealm.kt) or javadoc)

#### put()

Put object into Realm DB and return result (as Single).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - any Scheduler (e.g. Schedulers.io())

val newUser = UserDbItem(name = "Bob", age = 23, city = "Chicago")

db.put(newUser)
  .doOnSuccess {
      // do something
  }
  .subscribeOn(scheduler)
  .subscribe()
```

### UPDATE

Here are some of the methods for putting data in Realm DB.

(see all methods in [source](https://github.com/bornest/RxRealmKt/blob/master/lib/src/main/java/com/github/rxrealmkt/lib/RxRealm.kt) or javadoc)

#### update()

Update item in DB and return result (as Single).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - any Scheduler (e.g. Schedulers.io())
// user - unmanaged copy of UserDbItem

db.update(user) {
    city = "Los Angeles"
}
    .doOnSuccess {
        // do something
    }
    .subscribeOn(scheduler)
    .subscribe()

```

#### updateAll()

Update all items in DB that match the query and return result (as Single).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - any Scheduler (e.g. Schedulers.io())

db.updateAll<UserDbItem>(
    {
        equalTo(UserDbItem.cityColumn, "Leningrad")
    },
    {
        city = "St. Petersburg"
    }
)
    .doOnSuccess {
        // do something
    }
    .subscribeOn(scheduler)
    .subscribe()
```


### DELETE

Here are some of the methods for putting data in Realm DB.

(see all methods in [source](https://github.com/bornest/RxRealmKt/blob/master/lib/src/main/java/com/github/rxrealmkt/lib/RxRealm.kt) or javadoc)

#### deleteAll()

Delete all objects of the given class from Realm DB and return result (as Single).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - any Scheduler (e.g. Schedulers.io())

db.deleteAll<UserDbItem>()
    .doOnSuccess {
        // do something
    }
    .subscribeOn(scheduler)
    .subscribe()
```

#### delete()

Delete all objects of the given class from Realm DB that match the given query and return result (as Single).

Example:

```kotlin
// db - instance of RxRealm
// scheduler - any Scheduler (e.g. Schedulers.io())

db.delete<UserDbItem> {
    equalTo(UserDbItem.cityColumn, "New York")
    greaterThan(UserDbItem.ageColumn, 18)
}
    .doOnSuccess {
        // do something
    }
    .subscribeOn(scheduler)
    .subscribe()
```

### Transaction (advanced)

Usually get/put/update/delete methods are enough.
But if you want to do multiple operations in one transaction you can use `transaction()` method.

#### transaction()

Perform a DB transaction and return result (as Single).

```kotlin
// db - instance of RxRealm
// scheduler - any Scheduler (e.g. Schedulers.io())

val promocode = "15OFF"
val userEmail = "bob@gmail.com"

db.transaction {
    realm, success ->
    val discount = realm.where(DiscountDbItem::class.java)
        .equalTo(DiscountDbItem.expired, false)
        .equalTo(DiscountDbItem.promocodeColumn, promocode)
        .findFirst()

    if (discount != null) {
        val userCart = realm.where(ShoppingCartDbItem::class.java)
            .equalTo(ShoppingCartDbItem.userEmailColumn, userEmail)
            .findFirst()

        userCart.applyDiscount(discount.multiplier)
    } else {
        success.set(false)
    }
}
    .doOnSuccess {
      // do something
    }
    .doOnError {
      // do something else
    }
    .subscribeOn(scheduler)
    .subscribe()
```

## Important note

The library is in the early stages of development – there almost certainly will be
some breaking changes in the future.

Test coverage will be increased over time.

## License
```
Copyright (c) 2017, Borislav Nesterov

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

```
