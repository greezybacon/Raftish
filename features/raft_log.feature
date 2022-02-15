Feature: Raft has a compliant log according to section 5.3

    Scenario: Log is initially empty
        Given an empty raft log
         Then there are 0 items in the log

    Scenario: Log can handle an initial append
        Given an empty raft log
         When adding 1 random entry with term=1
         Then appendResult == True
          And there are 1 items in the log

    Scenario Outline: Append single items to the log
        Given an empty raft log
         When adding 10 random entry with term=2
          And adding 1 random entry with term=<term> and prev_index=<index>
         Then appendResult == <result>
          And there are <len> items in the log

        Examples: Successes
            | term      | index     | result    | len   |
            | 2         | 1         | True      | 10    |
            | 2         | 9         | True      | 11    |
            | 3         | 9         | True      | 11    |
            | 3         | 1         | True      | 3     |

        Examples: Failures
            | term      | index     | result    | len   |
            | 0         | 9         | False     | 10    |
            | 1         | 1         | False     | 10    |
            | 1         | 9         | False     | 10    |
            | 2         | 10        | False     | 10    |

    Scenario Outline: Append multiple items to the log
        Given an empty raft log
         When adding 10 random entry with term=2
          And adding <count> random entry with term=<term> and prev_index=<index>
         Then appendResult == <result>
          And there are <len> items in the log

        Examples: Successes
            | count | term      | index     | result    | len   |
            | 5     | 2         | 1         | True      | 10    |
            | 5     | 3         | 1         | True      | 7     |
            | 5     | 2         | 9         | True      | 15    |
            | 5     | 3         | 9         | True      | 15    |