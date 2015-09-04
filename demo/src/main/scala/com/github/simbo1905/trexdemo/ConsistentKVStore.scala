package com.github.simbo1905.trexdemo

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
   * The setting of the "consistent" flag is used
   * to choose the consistency level. Setting it to "true"
   * chooses strong consistency, and the latest value is always
   * returned. Setting it to "false" chooses timeline
   * consistency, and a possibly stale value is returned in
   * exchange for better performance.
   * @param key The key of the value to get
   * @return A tuple of the value and the version number of the value of the key
   */
  def get(key: String): Option[(String, Long)]
}
