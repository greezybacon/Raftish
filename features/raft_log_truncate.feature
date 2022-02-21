Feature: The TransactionLog can be truncated
    The transaction log, after being applied, can be truncated so that the
    memory footprint does not become a problem over the lifetime of the cluster.

    Scenario: Log can handle truncation and keep going
        Given an empty raft log
         When adding 1000 random entries with term=1
          And the log is truncated before 501
          And adding 1 random entry with term=1
         Then appendResult == True
          And there are 501 items in the log
          And the lastIndex is 1001
          And the startIndex is 501