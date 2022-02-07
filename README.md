# Rafting Trip - February 14-18, 2022

Hello! This is the course project repo for the "Rafting Trip"
course.  This project will serve as the central point of discussion, code
sharing, debugging, and other matters related to the project.

Although each person will work on their own code, it is requested
that all participants work from this central repo. First, create a
a branch for yourself:

    bash % git clone https://github.com/dabeaz-course/raft_2022_02
    bash % cd raft_2022_02
    bash % git checkout -b yourname
    bash % git push -u origin yourname

Do all of your subsequent work in your branch of this central repository. 

The use of GitHub makes it easier for me to look at your code and
answer questions (you can point me at your code, raise an issue,
etc.).  It also makes it easier for everyone else to look at your code
and to get ideas.  Implementing Raft is hard. Everyone is going to
have different ideas about the implementation, testing, and other
matters.  By having all of the code in one place, it will be better
and more of a shared experience.

I will also be using the repo to commit materials, solutions, and 
other things as the course date nears and during the course itself.

Finally, the repo serves as a good record for everything that happened
during the course after the fact.  

## Live Session

The course is conducted live from 09:30 to 17:30 US Central Time/CHICAGO
(UTC-06:00).  Here are details about the Zoom meeting:

Topic: Rafting Trip

Join Zoom Meeting
https://us02web.zoom.us/j/84678466137?pwd=VVFkaE13U3doZ2swWll1ZjR1UWJpdz09

Meeting ID: 846 7846 6137
Passcode: 886054

I will be in the meeting approximately 30 minutes prior to the start
time if you need to test your set up.

## Live Chat

For live chat during the course, we use Gitter.  Here is a link:

* [https://gitter.im/dabeaz-course/raft_2022_02](https://gitter.im/dabeaz-course/raft_2022_02)

## Raft Resources

Our primary source of information on Raft is found at
https://raft.github.io.  We will be working primarily from the [Raft Paper](https://raft.github.io/raft.pdf).
You should also watch the video presentation about Raft at https://www.youtube.com/watch?v=YbZ3zDzDnrw

## Preparation

Most participants find the project to be quite challenging.  Here are some preparation
details and study topics that might prove useful.

* Programming environment. You should be comfortable working with terminal sessions and
the command line.  The project involves running many different programs all at once--multiple
servers, clients, and so forth.  See this [short video](https://vimeo.com/401115908/a81c795591)
with some details about the setup.

* Networking. Raft involves sending messages between machines.  This
needs to be done with socket programming or some other kind of
messaging library.  I strongly advise looking into a basic tutorial on
networks for the programming language that you're going to use. Make
sure you look at the [Warmup](docs/Warmup.md) exercise.

* Concurrency.  A solution commonly involves a high degree of concurrency using threads,
tasks, or some similar construct. I would review some basics of thread programming (also covered in the
warmup exercise).

* The Actor Model and Communicating Sequential Processes. A useful program architecture
for Raft is to structure the project as a collection of communicating tasks. This is
something that is well-studied in distributed systems.  In particular, you might read
about the [Actor Model](https://en.wikipedia.org/wiki/Actor_model) as well as
[CSP](https://en.wikipedia.org/wiki/Communicating_sequential_processes).

* Object Oriented Programming. Although Raft can be implemented without writing
a gigantic amount of code, there are many parts that need to interact with each other.
Testing is also quite difficult. Programming patterns that allow different programming
concerns to be separated and tested independently will prove to be useful.
One such pattern is [Dependency Injection](https://en.wikipedia.org/wiki/Dependency_injection).

## The Challenge of Raft

One of the biggest challenges in the Raft project is knowing precisely
where to start.  As you read the [Raft paper](https://raft.github.io/raft.pdf), you should be thinking
about "where would I actually start with an implementation?"  There
are different facets that need to be addressed.  For example, at one
level, there's a networking layer involving machines and
messages. Maybe this is the starting point (at the low level).
However, there's also the "logic" of Raft (e.g., leader election
with leaders, followers, and candidates). So, an alternative might be to
approach from a high level instead.  And then there are applications
that might utilize Raft such as a key-value store database. Do you
build that first and then expand Raft on top of it?

The other challenge concerns testing and debugging.  How do you
implement it while having something that you can actually test and
verify?

## Preparation Exercises

The following project covers some background material related to socket
programming and concurrency that might help with the course:

* [Warmup Project](docs/Warmup.md)

The Raft project is similar to what might be taught in a graduate
distributed systems course such as MIT 6.824. A lot of information is
available on the course
[website](https://pdos.csail.mit.edu/6.824/index.html).

## Project Milestones

The following projects guide you through one possible path for implementing Raft.
This is not necessarily the only way to do it.  However, it's an approach
that has often worked in the past.

* [Project 1 - Key-Value Server](docs/Project1_KeyValue.md)
* [Project 2 - Event Driven Programming](docs/Project2_Events.md)
* [Project 3 - Starting Out/Planning](docs/Project3_Planning.md)
* [Project 4 - The Log](docs/Project4_TheLog.md)
* [Project 5 - Log Replication](docs/Project5_LogReplication.md)
* [Project 6 - Consensus](docs/Project6_Consensus.md)
* [Project 7 - Leader Election](docs/Project7_LeaderElection.md)
* [Project 8 - Testing](docs/Project8_Testing.md)
* [Project 9 - Simulation](docs/Project9_Simulation.md)
* [Project 10 - The System](docs/Project10_Systems.md)
* [Project 11 - The Rest of Raft](docs/Project11_Everything.md)

## Interesting Resources

* [Concurrency: the Works of Leslie Lamport](docs/documents/3335772.pdf) (PDF)
* [Implementing Raft, Eli Bendersky](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/) (blog)
* [Notes on Raft, Chelsea Troy](https://chelseatroy.com/tag/raft/) (blog)
* [The Cloudflare Failure](https://blog.cloudflare.com/a-byzantine-failure-in-the-real-world/)
* [The Google Outage of Dec 14, 2020](https://status.cloud.google.com/incident/zall/20013#20013004)
* [The AWS Server Event of Dec 7, 2021](https://aws.amazon.com/message/12721/)
* [The Roblox Outage of Oct 28-31, 2021](https://blog.roblox.com/2022/01/roblox-return-to-service-10-28-10-31-2021/)

## Live Session Videos

Video from the live session will be posted here.
