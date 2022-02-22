Feature: Raft can replicate log entries

    Scenario: Cluster will replicate log entries
        Given a cluster of 3 nodes
         When node 0 becomes the leader for term 1 
          And an log entry with "test-content" is added to the cluster log
         Then the last entry in all nodes is (1, 1)

    Scenario: Cluster will replicate the log
        Given a cluster of 3 nodes
          And the following log states
            """
            1,1,1,4,4,5,5,6,6,6,6,6
            1
            1
            """
         When node 0 becomes the leader for term 1 
          And the leader has replicated its logs to all nodes
          And node 1 will have log terms 1,1,1,4,4,5,5,6,6,6,6,6
          And node 1 is restarted
          And the leader has replicated its logs to all nodes
         Then node 1 will have log terms 1,1,1,4,4,5,5,6,6,6,6,6