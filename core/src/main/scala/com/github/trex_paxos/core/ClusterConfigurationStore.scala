package com.github.trex_paxos.core

import com.github.trex_paxos.ClusterConfiguration

trait ClusterConfigurationStore {
  def saveAndAssignEra(era: ClusterConfiguration): ClusterConfiguration

  /**
    * @return The latest cluster membership
    */
  def loadForHighestEra(): Option[ClusterConfiguration]
}
