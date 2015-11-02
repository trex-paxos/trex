package com.github.trex_paxos.demo

/**
 * Simple KV store with consistent read semantics inspired by the spinnaker paper
 * http://www.vldb.org/pvldb/vol4/p243-rao.pdf
 */
trait ConsistentKVStore {
  /**
   * Add a value into the KV store
   */
  def put(key: String, value: String): Unit

  /**
   * Add a value into the KV store only if the current version number is 'version'
   * @return True if the operation succeeded else False
   */
  def put(key: String, value: String, version: Long): Boolean

  /**
   * Remove a value form the store.
   */
  def remove(key: String): Unit

  /**
   * Remove a value from the KV store only if the current version number is "version"
   * @return
   */
  def remove(key: String, version: Long): Boolean

  /**
   * Read a value and its version number from the KV store.
   * @param key The key of the value to get.
   * @return A tuple of the value and the version number of the value of the key
   */
  def get(key: String): Option[(String, Long)]
}
