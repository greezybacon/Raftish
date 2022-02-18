Feature: Servers can elect a leader automatically among themselves

    Scenario: Leader election happens automatically
        Given a cluster of 3 nodes
         Then the cluster will have a leader
