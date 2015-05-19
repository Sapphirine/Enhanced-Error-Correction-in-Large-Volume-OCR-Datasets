# Enhanced-Error-Correction-in-Large-Volume-OCR-Datasets

The system requires a standard Spark installation and the Apache commons math jar specified on the command line, as shown below.
Before a shell can be started, generate an Ergo engine 

./bin/spark-submit --master local[*] --jars commons-math3-3.5.jar --class ee6895ta.ProcessData /clients/spark/sparktest.jar <csvfile> <tokenlen> init

Then induce the truth with
./bin/spark-submit --master local[*] --jars commons-math3-3.5.jar --class ee6895ta.ProcessData /clients/spark/sparktest.jar <csvfile> <tokenlen> infer

To start the shell:
./bin/spark-submit --master local[*] --jars commons-math3-3.5.jar --class ee6895ta.ErgoShell /clients/spark/sparktest.jar <csvfile>
The csv file is a simple file of the format token,freq
