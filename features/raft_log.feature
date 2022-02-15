Feature: Raft has a compliant log according to section 5.3

    Scenario: Log is initially empty
        Given an empty raft log
         Then there are 0 items in the log
          And commitIndex == 0

    Scenario: Log can handle an initial append
        Given an empty raft log
         When adding 1 random entry with term=1
         Then appendResult == True
          And there are 1 items in the log
          And commitIndex == 0

    Scenario Outline: Append single items to the log
        Given an empty raft log
         When adding 10 random entry with term=1
          And adding 1 random entry with term=<term> and prev_index=<index>
         Then appendResult == <result>
          And lastApplied == <lastApplied>
          And there are <len> items in the log

        Examples: Successes
            | term      | index     | result    | lastApplied   | len   |
            | 1         | 1         | True      | 2             | 3     |
            | 1         | 9         | True      | 10            | 11    |
            | 2         | 9         | True      | 10            | 11    |

        Examples: Failures
            | term      | index     | result    | lastApplied   | len   |
            | 0         | 9         | False     | 9             | 10    |
            | 1         | 10        | False     | 9             | 10    |

    Scenario Outline: Append multiple items to the log
        Given an empty raft log
         When adding 10 random entry with term=1
          And adding <count> random entry with term=<term> and prev_index=<index>
         Then appendResult == <result>
          And lastApplied == <lastApplied>
          And there are <len> items in the log

        Examples: Successes
            | count | term      | index     | result    | lastApplied   | len   |
            | 5     | 1         | 1         | True      | 6             | 7     |
            | 5     | 1         | 9         | True      | 14            | 15    |
            | 5     | 2         | 9         | True      | 14            | 15    |