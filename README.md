# Spark Order Book Creator

## Tested On
```
Spark : 3.5.0
Scala : 3.3.1
SBT : 1.9.7
```


## Dependencies
- Spark ([Documentation](https://spark.apache.org/docs/latest/))
- Scala ([Documentation](https://docs.scala-lang.org/))
- SBT ([Documentation](https://www.scala-sbt.org/1.x/docs/))


## How to Run
1. run unit test
    ``` bash
    $ sbt test
    ```
2. run assembly
    ``` bash
    $ sbt assembly
    ```
3. submit jar file to spark master
    ``` bash
    spark-submit
        --class org.anang.assessment.OrderBookCreator
        --master spark://localhost:7078
        --executor-memory 1g
        --total-executor-cores 2
        --conf spark.ui.reverseProxy=true
        --conf spark.ui.reverseProxyUrl=http://localhost:8080
        target/scala-2.12/OrderBookCreator-assembly-0.1.0.jar
    ```

It will generate order book every 1 second like this example:
![](https://github.com/annangsyarif/spark_sde_assessment/blob/main/output-example/picture_1.png)
![](https://github.com/annangsyarif/spark_sde_assessment/blob/main/output-example/picture_2.png)
