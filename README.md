# streamprocessor: transform streaming messages

## Prequisites
Install librdkafka:
```
brew install librdkafka
```

## Running an example
```
$ echo '{"test": 1, "a": 3, "c": 4}' | python3 example.py keys
```

## Running an example with Spark
```
PYSPARK_PYTHON=python3 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --py-files streamprocessor/ example_spark.py
```
