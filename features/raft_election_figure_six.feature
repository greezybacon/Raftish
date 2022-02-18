Feature: Leaders are elected as expected

    Background:
        Given a cluster of 5 nodes
          And the following log states
            """
            1,1,1,2,3,3,3,3,3
            1,1,1,2,3
            1,1,1,2,3,3,3,3,3
            1,1
            1,1,1,2,3,3,3
            """

        Scenario Outline: Only three of the five servers could win an election
             When node <candidate> becomes a candidate for term 4
             Then the cluster will have a leader
              And node <candidate> is_leader == <can_win>

            Examples:
                | candidate   | can_win |
                | 0           | True    |
                | 1           | False   |
                | 2           | True    |
                | 3           | False   |
                | 4           | True    |