for /f "delims=[] tokens=2" %%a in ('ping -4 -n 1 %ComputerName% ^| findstr [') do set NetworkIP=%%a
spark-class org.apache.spark.deploy.worker.Worker spark://%NetworkIP%:7077 -m 6g -c 3