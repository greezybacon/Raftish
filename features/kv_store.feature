Feature: Key-Value Store Can Save New Values

    Scenario: Initial database is empty
        Given an empty kv database
         Then abc is not in the database

    Scenario: Add new value to the database
        Given an empty kv database
         When abc=123 is added
         Then abc is in the database
          And the value of abc is 123

    Scenario: Items can be removed from the database
        Given an empty kv database
         When abc=456 is added
          And abc is deleted from the database
         Then abc is not in the database
