Feature: Database values can be added and queried via the server

    Scenario: Values can be added through the server API
        Given a kv server
          And a kv client
         When abc=123 is added
         Then abc is in the database

    Scenario: Values can be removed through the server API
        Given a kv server
          And a kv client
         When abc=123 is added
          And abc is deleted from the database
         Then abc is not in the database