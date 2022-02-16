Feature: Cluster can recover from the state in Figure 7

    Background: 
        Given a cluster of 7 nodes
          And the following log states
            """
            1,1,1,4,4,5,5,6,6,6
            1,1,1,4,4,5,5,6,6
            1,1,1,4
            1,1,1,4,4,5,5,6,6,6,6
            1,1,1,4,4,5,5,6,6,6,7,7
            1,1,1,4,4,4,4
            1,1,1,2,2,2,3,3,3,3,3
            """

        Scenario: Node 0, if elected will replicate its log
            When node 0 becomes the leader for term 8
             And an log entry with "test-content" is added to the cluster log
             And the leader has replicated its logs to all nodes
            Then all nodes will have log with terms "1,1,1,4,4,5,5,6,6,6,8"

        Scenario: Node 1, if elected will replicate its log
            When node 1 becomes the leader for term 8
             And an log entry with "test-content" is added to the cluster log
             And the leader has replicated its logs to all nodes
            Then all nodes will have log with terms "1,1,1,4,4,5,5,6,6,8"
