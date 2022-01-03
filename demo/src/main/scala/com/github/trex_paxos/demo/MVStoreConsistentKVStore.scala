package com.github.trex_paxos.demo

import org.h2.mvstore.{MVMap, MVStore}

/**
 * This class is not threadsafe as it writes to two data structures before committing.
 * @param store
 */
class MVStoreConsistentKVStore(store: MVStore) extends ConsistentKVStore {
  require(store != null)

  val valueStore: MVMap[String, String] = store.openMap("trexdemovalues")

  val versionStore: MVMap[String, Long] = store.openMap("trexdemoversions")

  /**
   * Add a value into the KV store
   */
  override def put(key: String, value: String): Unit = {
    val currentVersion = versionStore.getOrDefault(key, 0L)
    versionStore.put(key, currentVersion + 1)
    valueStore.put(key, value)
    store.commit()
  }

  /**
   * Read a value and its version number from the KV store.
   *
   * @param key The key of the value to get.
   * @return A tuple of the value and the version number of the value of the key
   */
  override def get(key: String): Option[(String, Long)] =
    versionStore.getOrDefault(key, -1) match {
      case -1 =>
        None
      case v =>
        Option(valueStore.get(key) -> v)
    }

  /**
   * Add a value into the KV store only if the current version number is 'version'
   *
   * @return True if the operation succeeded else False
   */
  override def put(key: String, value: String, version: Long): Boolean =
    versionStore.getOrDefault(key, Int.MinValue) match {
      case Int.MinValue if version == 0 => // value not in store is implicitly version=0
        versionStore.put(key, 1)
        valueStore.put(key, value)
        store.commit()
        true
      case currentVersion if currentVersion == version =>
        versionStore.put(key, currentVersion + 1)
        valueStore.put(key, value)
        store.commit()
        true
      case _ =>
        false
    }

  /**
   * Remove a value form the store.
   */
  override def remove(key: String): Unit = {
    versionStore.remove(key)
    valueStore.remove(key)
    store.commit()
  }


  /**
   * Remove a value from the KV store only if the current version number is "version"
   *
   * @return
   */
  override def remove(key: String, version: Long): Boolean = versionStore.getOrDefault(key, -1) match {
    case -1 =>
      false
    case currentVersion if currentVersion == version =>
      versionStore.remove(key)
      valueStore.remove(key)
      store.commit()
      true
    case _ =>
      false
  }


}
