package com.tradeshift.blayze.collection

import org.lmdbjava.Env.create
import java.io.File
import org.lmdbjava.DbiFlags.MDB_CREATE
import java.lang.Integer.BYTES
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import kotlin.text.Charsets.UTF_8
import org.lmdbjava.*
import java.util.*

val DB_NAME = "my DB"
/**
 * A table implementation based on a Map<Pair<String, String>, Int>
 */
class HDTable(
        val map: Map<Pair<String, String>, Int> = mapOf(),
        var env: Env<ByteBuffer>? = null,
        override val rowKeySet: MutableSet<String> = mutableSetOf(),
        override val columnKeySet: MutableSet<String> = mutableSetOf()
) : Table {

    val db: Dbi<ByteBuffer>

    init {
        if (env == null) {
            val path = File("somedbfile/" + UUID.randomUUID())
            path.mkdirs()
            env = create()
                    // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                    .setMapSize(10_485_760)
                    // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                    .setMaxDbs(1)
                    // Now let's open the Env. The same path can be concurrently opened and
                    // used in different processes, but do not open the same path twice in
                    // the same process at the same time.
                    .setMaxReaders(1000000)
                    .open(path)
        }
        db = env!!.openDbi(DB_NAME, MDB_CREATE)

        if (!map.isEmpty()) {
            map.entries.forEach {
                db.put(bb(it.key.first + "|" + it.key.second), bb(it.value))
                rowKeySet.add(it.key.first)
                columnKeySet.add(it.key.second)
            }

            // TODO check speed ...
//            for (a in 1..100_000) {
//                db.put(bb(UUID.randomUUID().toString()), bb(20))
//            }
        }
    }

    override val entries: Set<Map.Entry<Pair<String, String>, Int>> by lazy {
        env!!.txnRead().use { txn ->
            db.iterate(txn, KeyRange.all()).use { it ->
                val map = mutableMapOf<Pair<String, String>, Int>()
                it.iterable().forEach {
                    val combinedKeys = UTF_8.decode(it.key()).toString().split("|")
                    map.put(
                            combinedKeys.get(0) to combinedKeys.get(1),
                            it.`val`().int )
                }

                return@lazy map.entries
            }
        }
    }

    override operator fun get(k1: String, k2: String): Int? {
        env!!.txnRead().use { txn ->
            val found = db!!.get(txn, bb(k1 + "|" + k2))
            found ?: return null

            // The fetchedVal is read-only and points to LMDB memory
            val fetchedVal = txn.`val`()

            // Let's double-check the fetched value is correct
            return fetchedVal.int
        }
    }

    override fun toMutableTable(): MutableTable {
        return MutableHDTable(env, rowKeySet = rowKeySet, columnKeySet = columnKeySet)
    }

    override fun toTable(): Table {
        return this
    }
}

/**
 * A mutable table implementation based on a Map<Pair<String, String>, Int>
 */
class MutableHDTable(
        var env: Env<ByteBuffer>? = null,
        override val rowKeySet: MutableSet<String> = mutableSetOf(),
        override val columnKeySet: MutableSet<String> = mutableSetOf()
) : MutableTable{

    val db: Dbi<ByteBuffer>
    init {
        if (env == null) {
            val path = File("somedbfile/" + UUID.randomUUID())
            path.mkdirs()
            env = create()
                    // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                    .setMapSize(10_485_760)
                    // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                    .setMaxDbs(1)
                    // Now let's open the Env. The same path can be concurrently opened and
                    // used in different processes, but do not open the same path twice in
                    // the same process at the same time.
                    .setMaxReaders(1000000)
                    .open(path)
        }
        db = env!!.openDbi(DB_NAME, MDB_CREATE)
    }

    override val entries: Set<Map.Entry<Pair<String, String>, Int>> by lazy {
        env!!.txnRead().use { txn ->
            db.iterate(txn, KeyRange.all()).use { it ->
                val map = mutableMapOf<Pair<String, String>, Int>()
                it.iterable().forEach {
                    val combinedKeys = UTF_8.decode(it.key()).toString().split("|")
                    map.put(
                            combinedKeys.get(0) to combinedKeys.get(1),
                            it.`val`().int )
                }

                return@lazy map.entries
            }
        }
    }

    override operator fun get(k1: String, k2: String): Int? {
        env!!.txnRead().use { txn ->
            val found = db!!.get(txn, bb(k1 + "|" + k2))
            found ?: return null

            // The fetchedVal is read-only and points to LMDB memory
            val fetchedVal = txn.`val`()

            // Let's double-check the fetched value is correct
            return fetchedVal.int
        }
    }

    override fun put(rowKey: String, columnKey: String, value: Int) {
        env!!.txnWrite().use { txn ->
            db.put(txn, bb(rowKey + "|" + columnKey), bb(value))
            txn.commit()

            rowKeySet.add(rowKey)
            columnKeySet.add(columnKey)
        }
    }

    override fun toTable(): Table {
        return HDTable(env = this.env, rowKeySet = rowKeySet, columnKeySet = columnKeySet)
    }

    override fun toMutableTable(): MutableTable {
        return this
    }
}

fun bb(value: Int): ByteBuffer {
    val bb = allocateDirect(BYTES)
    bb.putInt(value).flip()
    return bb
}

fun bb(value: String): ByteBuffer {
    val bb = allocateDirect(700)
    bb.put(value.toByteArray(UTF_8)).flip()
    return bb
}

fun tableOf(pairs: Iterable<Pair<Pair<String, String>, Int>>): Table = HDTable(pairs.toMap())
fun tableOf(vararg pairs: Pair<Pair<String, String>, Int>): Table = HDTable(pairs.toMap())