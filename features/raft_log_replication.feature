Feature: Raft can replicate log entries

    Scenario:
        Given a cluster of 3 nodes
         When node 0 becomes the leader for term 1 
          And an log entry with "test-content" is added to the cluster log
         Then the last entry in all nodes is (1, 1)

    Scenario: Cluster will replicate the log
        Given a cluster of 3 nodes
          And a raft log with terms 1,1,1,4,4,5,5,6,6,6,6,6
         When node 0 is restarted
          And the leader has replicated its logs to all nodes
         Then node 0 will have log terms 1,1,1,4,4,5,5,6,6,6,6,6