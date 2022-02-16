Feature: Raft can replicate log entries

    Scenario:
        Given a cluster of 3 nodes
         When node 0 becomes the leader for term 1 
          And an log entry with "test-content" is added to the cluster log
         Then the last entry in all nodes is (1, 0)
