Feature: Raft has a compliant log according to section 5.3

    Scenario: Log is initially empty
        Given an empty raft log
         Then there are 0 items in the log

    Scenario: Log can handle an initial append
        Given an empty raft log
         When adding 1 random entry with term=1
         Then appendResult == True
          And there are 1 items in the log

    Scenario: Log is cleared when replacing an item with a different term
        Given a raft log with terms 1,1,1,4,4,5,5,6
         When adding 1 entry with term=1 at index=3
         Then appendResult == True
          And the log will have terms 1,1,1,1

    Scenario Outline: Append single items to the log
        Given an empty raft log
         When adding 10 random entry with term=2
          And adding 1 random entry with term=<term> and prev_index=<index>
         Then appendResult == <result>
          And there are <len> items in the log

        Examples: Successes
            | term      | index     | result    | len   |
            | 0         | 0         | True      | 1     |
            | 2         | 0         | True      | 10    |
            | 2         | 2         | True      | 10    |
            | 2         | 10        | True      | 11    |
            | 3         | 10        | True      | 11    |
            | 3         | 2         | True      | 3     |

        Examples: Failures
            | term      | index     | result    | len   |
            | 0         | 9         | False     | 10    |
            | 1         | 2         | False     | 10    |
            | 1         | 10        | False     | 10    |
            | 2         | 11        | False     | 10    |

    Scenario Outline: Append multiple items to the log
        Given an empty raft log
         When adding 10 random entry with term=2
          And adding <count> random entry with term=<term> and prev_index=<index>
         Then appendResult == <result>
          And there are <len> items in the log

        Examples: Successes
            | count | term      | index     | result    | len   |
            | 5     | 2         | 2         | True      | 10    |
            | 5     | 3         | 2         | True      | 7     |
            | 5     | 2         | 10        | True      | 15    |
            | 5     | 3         | 10        | True      | 15    |