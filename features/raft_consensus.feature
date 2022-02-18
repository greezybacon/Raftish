Feature: The cluster will reach consensus after appending log entries

    Scenario: Basic append to cluster
        Given a cluster of 3 nodes
         When node 0 becomes the leader for term 1 
          And after a pause of 0.05 seconds
          And an log entry with "test-content" is added to the cluster log
          And log entry 1 is committed
         Then the leaderCommitId will be 1  