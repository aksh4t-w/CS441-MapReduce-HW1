# CS441 Homework-1
## University of Illinois at Chicago
## Akshat Wagadre
## UIN - 654098103

## Introduction
This project implements different Map-Reduce tasks on a bunch of log files using the Hadoop framework.
It is later executed on AWS EMR to explore Map-Reduce on distributed objects in cloud.

## Prerequisites
* Java: JDK 18.0
* Hadoop 3.3.4
* SBT

## Installation
1. Clone the project from this repository. URL: https://github.com/aksh4t-w/CS441-MapReduce-HW1.git
2. Navigate to the project folder using your terminal or open the project in IntelliJ.
3. Run the command: sbt clean compile assembly to build the JAR file. The JAR will be located in the target/scala-3.2.0 folder.
4. The project has 4 classes located in the package CS441HW1. Each class has 1 task. 
5. Task 1 takes 2 paramenters: the input and the output path. 
6. Task 2 takes 3 paramenters: the input, the temp input and the output path. 
7. Task 3 takes 2 paramenters: the input and the output path.
8. Task 4 takes 2 paramenters: the input and the output path.
9. The pattern and time interval parameters are defined in the config file in the "src/main/resources" folder.
10. A demo for running this project on EMR can be found here: https://www.youtube.com/watch?v=oxoc9e2yLkw&t=665s

## Tasks
### 1. MR_Task1.scala
This map reduce task takes in log files in a folder as input and outputs the number of messages of each log type
(INFO, DEBUG, WARN, ERROR) divided across time intervals of n seconds where n is passed as a parameter in the log file during
runtime.

|    Class    |                                   Utility                                   |
|:-----------:|:---------------------------------------------------------------------------:|
| TokenMapper | Pattern match and assign groups to each log message based on time intervals |
| LogReducer  |           Writes the log type, interval and count to the context            |

### 2. MR_Task2.scala
This map reduce task takes in log files in a folder as input and outputs the time intervals sorted
in descending order that contained most log messages of the type ERROR with injected regex pattern string instances.
It makes use of two map reduce operations and one comparator operator for sorting the result based on the keys which
are the count of specified logs given.

|    Class     |                                           Utility                                           |
|:------------:|:-------------------------------------------------------------------------------------------:|
| TokenMapper  |         Pattern match and assign groups to each log message based on time intervals         |
 |  LogReducer  |                   Writes the log type, interval and count to the context                    |
| FinalMapper  | Converts the integer time back to date time format and writes count of matched logs as keys |
|  Comparator  |             Overrides the compare function to sort the keys in descending order             |
| FinalReducer |               Writes the time interval and count of the pattern matched logs                |

### 3. MR_Task3.scala
This map reduce task takes in log files in a folder as input and outputs the number of messages of each log type
(INFO, DEBUG, WARN, ERROR) in all the generated logs.

|    Class    |                                       Utility                                       |
|:-----------:|:-----------------------------------------------------------------------------------:|
| TokenMapper |         Writes the log type to the context if a log of given type is found          |
| LogReducer  | Writes the count of each log type by summing up the values returned by the mappers  |


### 4. MR_Task4.scala
This map reduce task takes in log files in a folder as input, finds the logs with the matching regex pattern given
and outputs the maximum length of the pattern for all four log types. The mapper maps matches the pattern and sends
the count of each pattern to the reducer which later finds the maximum out of those lengths and writes it in the output
for corresponding log types.

|    Class    |                                        Utility                                        |
|:-----------:|:-------------------------------------------------------------------------------------:|
| TokenMapper |           Writes the length of patterns in the matched logs to the context            |
| LogReducer  | Writes the log types with their corresponding maximum length for the matched patterns |

## AWS Deployment
The JAR file generated using sbt assembly is uploaded to AWS S3 bucket and later used in the EMR cluster as shown in the YouTube video.
An EMR cluster needs to be setup and in each step, we need to pass the input arguments as: (task name) (input path) (optional input path) (output path) as required by the task.