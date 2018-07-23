package com.tradeshift.blayze.collection

/**
 * A table class which can be used to store and access value via a String and a String
 */

interface Table{
    val entries: Set<Map.Entry<Pair<String, String>, Int>>
    val rowKeySet: Set<String>
    val columnKeySet: Set<String>

    operator fun get(k1: String, k2: String): Int?
    fun toMutableTable(): MutableTable
    fun toTable(): Table
}

interface MutableTable : Table {
    fun put(rowKey: String, columnKey: String, value: Int)

}

/**
 * A table implementation based on a Map<Pair<String, String>, Int>
 */
class MapTable(val map: Map<Pair<String, String>, Int> = mapOf()) : Table {
    override val entries: Set<Map.Entry<Pair<String, String>, Int>> = map.entries // .map { (k, v) -> k.first.toString() + k.second.toString() to v }.toSet()

    override val rowKeySet: Set<String> by lazy { // as table is immutable, we use lazy delegates to only compute the property once on first access
        map.keys.map { it.first }.toSet()
    }

    override val columnKeySet: Set<String> by lazy {
        map.keys.map { it.second }.toSet()
    }

    override operator fun get(k1: String, k2: String) = map[k1 to k2]

    override fun toMutableTable(): MutableTable {
        return MutableMapTable(map.toMutableMap())
    }

    override fun toTable(): Table {
        return this
    }
}

/**
 * A mutable table implementation based on a Map<Pair<String, String>, Int>
 */
class MutableMapTable(
        val map: MutableMap<Pair<String, String>, Int> = mutableMapOf()
) : MutableTable{

    override val entries
        get() = map.entries // .map { (k, v) -> k.first.toString() + k.second.toString() to v }.toSet()

    override val rowKeySet: Set<String>
        get() = map.keys.map { it.first }.toSet()

    override val columnKeySet: Set<String>
        get() = map.keys.map { it.second }.toSet()

    override operator fun get(k1: String, k2: String) = map[k1 to k2]

    override fun put(rowKey: String, columnKey: String, value: Int) {
        map[rowKey to columnKey] = value
    }

    override fun toTable(): Table {
        return MapTable(map.toMap())
    }

    override fun toMutableTable(): MutableTable {
        return this
    }
}

//fun <String, String> tableOf(pairs: Iterable<Pair<Pair<String, String>, Int>>): Table = MapTable(pairs.toMap())
//fun <String, String> tableOf(vararg pairs: Pair<Pair<String, String>, Int>): Table = MapTable(pairs.toMap())
//fun <String, String> mutableTableOf(vararg pairs: Pair<Pair<String, String>, Int>): MutableTable = MutableMapTable(pairs.toMap().toMutableMap())
