# pyspark-vehicle-sales

This project explores and analyzes a car sales dataset using Apache Spark. It is designed for learning purposes and demonstrates the capabilities of PySpark for data manipulation, aggregation, and visualization.

The dataset that has been used can be found at: [https://www.kaggle.com/datasets/syedanwarafridi/vehicle-sales-data](https://www.kaggle.com/datasets/syedanwarafridi/vehicle-sales-data)

## Installing PySpark on Linux

1. If Java is not installed, you must install befor install pyspark.
```
sudo apt install default-jdk
```

2. Install Apache Spark
```
wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
tar -xvzf spark-3.2.0-bin-hadoop3.2.tgz
sudo mv spark-3.2.0-bin-hadoop3.2 /opt/spark
```

3. Set Up Environment Variables
```
sudo nano ~/.bashrc
```
```
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

4. Install PySpark
```
make install
```

> :warning: Make sure that the version of spark installed in your machine does match the version of pyspark.

