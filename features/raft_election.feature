Feature: Servers can elect a leader automatically among themselves

    Scenario: Leader election happens automatically
        Given a cluster of 3 nodes
         Then the cluster will have a leader

    Scenario: The cluster can elect a leader with multiple candidates
        Given a cluster of 3 nodes
         When node 0 becomes a candidate
          And node 1 becomes a candidate
         Then the cluster will have a leader
          And the cluster will have 2 followers

    Scenario: The cluster can elect a leader with multiple candidates
        Given a cluster of 3 nodes
         When node 0 becomes a candidate for term 2
          And node 1 becomes a candidate for term 2
          And node 2 becomes a candidate for term 2
         Then the cluster will have a leader
          And the cluster will have 2 followers
       
    Scenario: The candidate with the newest term will win
        Given a cluster of 3 nodes
         When node 0 becomes a candidate for term 4
          And node 1 becomes a candidate for term 5
         Then the cluster will have a leader
          And node 0 is not the leader
          And node 1 is the leader

    Scenario: A node cannot vote twice in one term
        Given a raft server
         When voting for node 1 for term 2
         Then voting for node 2 for term 2 will fail
          But voting for node 2 for term 3 will succeed