# Doran 🪙

_Effrem E. Doran is a non-playable character in League of Legends,
known for designing three starter items, including Wukong's staff,
praised as a "masterpiece."_

_The reference was taken from the article "[Doran]" on the [League of Legends] wiki on [FANDOM]._

[Doran]: https://leagueoflegends.fandom.com/pt-br/wiki/Doran
[League of Legends]: https://www.leagueoflegends.com
[FANDOM]: https://fandom.com

## Context

TBD
GraphX
https://spark.apache.org/graphx/
https://spark.apache.org/docs/latest/graphx-programming-guide.html


## How to use this project?

To build and execute this project you need to have installed:

- Java (17)
- Scala (2.12)
- SBT (1.10)
- Apache Spark (3.5.3)

> 💡 _The values in parenthesis are the versions I'm using._

The rest is very simple, I've created a `Makefile` to help you.

## Learnings

In this section I'll save some interesting things I've learned during this project.

### How to improve Spark Logs?

This is a important one 😅
Spark writes a lot of logs by default, which makes harder to see your own logs.

For that my suggestion is to configure your Spark with [this log file].

[this log file]: https://gist.github.com/avcaliani/f20b9d9a3ee2fb4f15d404905e143afa

### How to create a scala project using SBT?

After having installed, Java, Scala and SBT you can start a new project by doing the following.

```bash
sbt new scala/hello-world.g8
```
