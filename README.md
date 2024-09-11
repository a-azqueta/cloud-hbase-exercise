# cloud-hbase-exercise
This repository contains the solution to the exercises proposed in the Cloud Computing and Big Data Ecosystems course at the Universidad Politécnica de Madrid on the module Apache HBase.

Exercise Statement
Design and implement an application that manages users and user sessions. 
User information we need to keep: UserId, Address, PhoneNumber, UserName, UserLastName, Province, Session(s)-> Log in date/hour
Application stores user log in time
Design the HBase schema needed for modeling this application taking into cosideration the following queries:

* Query 0:  Check if user with UserName “Romeo” and LastName “Rodriguez” with Id = 1 exists 
* Query 1: Count all the Users 
* Query 2: Query all the Users with LastName = “Rodriguez”
* Query 3: Query all the Users with LastName = “Rodriguez” from Province = Soria
* Query 4: List the last 3 sessions from a given user. 
