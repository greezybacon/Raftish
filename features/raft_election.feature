Feature: Servers can elect a leader automatically among themselves

    Scenario: Leader election happens automatically
        Given a cluster of 3 nodes
         When after a pause of 0.5 seconds
         Then the cluster will have a leader
