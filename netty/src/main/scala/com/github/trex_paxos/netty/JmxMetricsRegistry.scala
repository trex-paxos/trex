package com.github.trex_paxos.netty

import com.codahale.metrics.{JmxReporter, MetricRegistry}

object JmxMetricsRegistry {
  val registry = new MetricRegistry()
  JmxReporter.forRegistry(registry).build().start()
}
