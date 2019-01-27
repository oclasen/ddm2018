Implementation for DDM2018 Task3 Flink Stream

The functionality is implemented in the StreamingJob class. The passed log is analysed for the most often appearing client and request and the biggest requested resource. The path to the log can be passed by either "-p" or "--path". The number of cores can be passed with "-c" or "--cores". 

Sadly the dependencies created by the flink skeleton project do not work, therefore our jar does not work. After hours of googling we capitulated. If you execute the main class in IntelliJ, everything works fine. The jar does not work because of ClassNotFoundException: org.slf4j.LoggerFactory.

We are looking forward for your hints.