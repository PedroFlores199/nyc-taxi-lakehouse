# Plan de ejecución antes y después de optimizar

Consulta: viajes e importe medio por distrito, zona de servicio y hora entre 2024-01-15 y 2024-01-21.
Ambas versiones devuelven las mismas 205 filas.
Tiempos: mediana de 10 ejecuciones.

| Versión | Tiempo (ms) |
|---|---|
| Antes: sin broadcast, filtro sobre una columna calculada | 23.55 |
| Después: broadcast + poda de particiones + proyección | 18.62 |

## Antes

```
== Physical Plan ==
AdaptiveSparkPlan (34)
+- == Final Plan ==
   * HashAggregate (21)
   +- AQEShuffleRead (20)
      +- ShuffleQueryStage (19), Statistics(sizeInBytes=53.5 KiB, rowCount=672)
         +- Exchange (18)
            +- * HashAggregate (17)
               +- * Project (16)
                  +- * SortMergeJoin Inner (15)
                     :- * Sort (8)
                     :  +- AQEShuffleRead (7)
                     :     +- ShuffleQueryStage (6), Statistics(sizeInBytes=18.8 MiB, rowCount=6.17E+5)
                     :        +- Exchange (5)
                     :           +- * Project (4)
                     :              +- * Filter (3)
                     :                 +- * ColumnarToRow (2)
                     :                    +- Scan parquet  (1)
                     +- * Sort (14)
                        +- AQEShuffleRead (13)
                           +- ShuffleQueryStage (12), Statistics(sizeInBytes=15.1 KiB, rowCount=265)
                              +- Exchange (11)
                                 +- * Filter (10)
                                    +- Scan csv  (9)
+- == Initial Plan ==
   HashAggregate (33)
   +- Exchange (32)
      +- HashAggregate (31)
         +- Project (30)
            +- SortMergeJoin Inner (29)
               :- Sort (25)
               :  +- Exchange (24)
               :     +- Project (23)
               :        +- Filter (22)
               :           +- Scan parquet  (1)
               +- Sort (28)
                  +- Exchange (27)
                     +- Filter (26)
                        +- Scan csv  (9)


(1) Scan parquet 
Output [5]: [pickup_location_id#1, pickup_datetime#3, total_amount#11, pickup_hour#14, pickup_date#25]
Batched: true
Location: InMemoryFileIndex [file:/Users/pedro/.gemini/antigravity/scratch/nyc-taxi-lakehouse/data/silver/trips]
PushedFilters: [IsNotNull(pickup_datetime), GreaterThanOrEqual(pickup_datetime,2024-01-15 00:00:00.0), LessThan(pickup_datetime,2024-01-22 00:00:00.0), IsNotNull(pickup_location_id)]
ReadSchema: struct<pickup_location_id:int,pickup_datetime:timestamp,total_amount:double,pickup_hour:int>

(2) ColumnarToRow [codegen id : 1]
Input [5]: [pickup_location_id#1, pickup_datetime#3, total_amount#11, pickup_hour#14, pickup_date#25]

(3) Filter [codegen id : 1]
Input [5]: [pickup_location_id#1, pickup_datetime#3, total_amount#11, pickup_hour#14, pickup_date#25]
Condition : (((isnotnull(pickup_datetime#3) AND (pickup_datetime#3 >= 2024-01-15 00:00:00)) AND (pickup_datetime#3 < 2024-01-22 00:00:00)) AND isnotnull(pickup_location_id#1))

(4) Project [codegen id : 1]
Output [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Input [5]: [pickup_location_id#1, pickup_datetime#3, total_amount#11, pickup_hour#14, pickup_date#25]

(5) Exchange
Input [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Arguments: hashpartitioning(pickup_location_id#1, 8), ENSURE_REQUIREMENTS, [plan_id=98]

(6) ShuffleQueryStage
Output [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Arguments: 0

(7) AQEShuffleRead
Input [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Arguments: coalesced

(8) Sort [codegen id : 3]
Input [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Arguments: [pickup_location_id#1 ASC NULLS FIRST], false, 0

(9) Scan csv 
Output [3]: [LocationID#69, Borough#70, service_zone#72]
Batched: false
Location: InMemoryFileIndex [file:/Users/pedro/.gemini/antigravity/scratch/nyc-taxi-lakehouse/data/bronze/zones/taxi_zone_lookup.csv]
PushedFilters: [IsNotNull(LocationID)]
ReadSchema: struct<LocationID:int,Borough:string,service_zone:string>

(10) Filter [codegen id : 2]
Input [3]: [LocationID#69, Borough#70, service_zone#72]
Condition : isnotnull(LocationID#69)

(11) Exchange
Input [3]: [LocationID#69, Borough#70, service_zone#72]
Arguments: hashpartitioning(LocationID#69, 8), ENSURE_REQUIREMENTS, [plan_id=113]

(12) ShuffleQueryStage
Output [3]: [LocationID#69, Borough#70, service_zone#72]
Arguments: 1

(13) AQEShuffleRead
Input [3]: [LocationID#69, Borough#70, service_zone#72]
Arguments: coalesced

(14) Sort [codegen id : 4]
Input [3]: [LocationID#69, Borough#70, service_zone#72]
Arguments: [LocationID#69 ASC NULLS FIRST], false, 0

(15) SortMergeJoin [codegen id : 5]
Left keys [1]: [pickup_location_id#1]
Right keys [1]: [LocationID#69]
Join type: Inner
Join condition: None

(16) Project [codegen id : 5]
Output [4]: [total_amount#11, pickup_hour#14, Borough#70, service_zone#72]
Input [6]: [pickup_location_id#1, total_amount#11, pickup_hour#14, LocationID#69, Borough#70, service_zone#72]

(17) HashAggregate [codegen id : 5]
Input [4]: [total_amount#11, pickup_hour#14, Borough#70, service_zone#72]
Keys [3]: [Borough#70, service_zone#72, pickup_hour#14]
Functions [2]: [partial_count(1), partial_avg(total_amount#11)]
Aggregate Attributes [3]: [count#178L, sum#179, count#180L]
Results [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]

(18) Exchange
Input [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]
Arguments: hashpartitioning(Borough#70, service_zone#72, pickup_hour#14, 8), ENSURE_REQUIREMENTS, [plan_id=205]

(19) ShuffleQueryStage
Output [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]
Arguments: 2

(20) AQEShuffleRead
Input [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]
Arguments: coalesced

(21) HashAggregate [codegen id : 6]
Input [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]
Keys [3]: [Borough#70, service_zone#72, pickup_hour#14]
Functions [2]: [count(1), avg(total_amount#11)]
Aggregate Attributes [2]: [count(1)#169L, avg(total_amount#11)#171]
Results [5]: [Borough#70, service_zone#72, pickup_hour#14, count(1)#169L AS trips#170L, round(avg(total_amount#11)#171, 2) AS avg_amount#172]

(22) Filter
Input [5]: [pickup_location_id#1, pickup_datetime#3, total_amount#11, pickup_hour#14, pickup_date#25]
Condition : (((isnotnull(pickup_datetime#3) AND (pickup_datetime#3 >= 2024-01-15 00:00:00)) AND (pickup_datetime#3 < 2024-01-22 00:00:00)) AND isnotnull(pickup_location_id#1))

(23) Project
Output [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Input [5]: [pickup_location_id#1, pickup_datetime#3, total_amount#11, pickup_hour#14, pickup_date#25]

(24) Exchange
Input [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Arguments: hashpartitioning(pickup_location_id#1, 8), ENSURE_REQUIREMENTS, [plan_id=55]

(25) Sort
Input [3]: [pickup_location_id#1, total_amount#11, pickup_hour#14]
Arguments: [pickup_location_id#1 ASC NULLS FIRST], false, 0

(26) Filter
Input [3]: [LocationID#69, Borough#70, service_zone#72]
Condition : isnotnull(LocationID#69)

(27) Exchange
Input [3]: [LocationID#69, Borough#70, service_zone#72]
Arguments: hashpartitioning(LocationID#69, 8), ENSURE_REQUIREMENTS, [plan_id=56]

(28) Sort
Input [3]: [LocationID#69, Borough#70, service_zone#72]
Arguments: [LocationID#69 ASC NULLS FIRST], false, 0

(29) SortMergeJoin
Left keys [1]: [pickup_location_id#1]
Right keys [1]: [LocationID#69]
Join type: Inner
Join condition: None

(30) Project
Output [4]: [total_amount#11, pickup_hour#14, Borough#70, service_zone#72]
Input [6]: [pickup_location_id#1, total_amount#11, pickup_hour#14, LocationID#69, Borough#70, service_zone#72]

(31) HashAggregate
Input [4]: [total_amount#11, pickup_hour#14, Borough#70, service_zone#72]
Keys [3]: [Borough#70, service_zone#72, pickup_hour#14]
Functions [2]: [partial_count(1), partial_avg(total_amount#11)]
Aggregate Attributes [3]: [count#178L, sum#179, count#180L]
Results [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]

(32) Exchange
Input [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]
Arguments: hashpartitioning(Borough#70, service_zone#72, pickup_hour#14, 8), ENSURE_REQUIREMENTS, [plan_id=63]

(33) HashAggregate
Input [6]: [Borough#70, service_zone#72, pickup_hour#14, count#181L, sum#182, count#183L]
Keys [3]: [Borough#70, service_zone#72, pickup_hour#14]
Functions [2]: [count(1), avg(total_amount#11)]
Aggregate Attributes [2]: [count(1)#169L, avg(total_amount#11)#171]
Results [5]: [Borough#70, service_zone#72, pickup_hour#14, count(1)#169L AS trips#170L, round(avg(total_amount#11)#171, 2) AS avg_amount#172]

(34) AdaptiveSparkPlan
Output [5]: [Borough#70, service_zone#72, pickup_hour#14, trips#170L, avg_amount#172]
Arguments: isFinalPlan=true
```

## Después

```
== Physical Plan ==
AdaptiveSparkPlan (27)
+- == Final Plan ==
   * HashAggregate (16)
   +- AQEShuffleRead (15)
      +- ShuffleQueryStage (14), Statistics(sizeInBytes=95.4 KiB, rowCount=1.23E+3)
         +- Exchange (13)
            +- * HashAggregate (12)
               +- * Project (11)
                  +- * BroadcastHashJoin Inner BuildRight (10)
                     :- * Project (4)
                     :  +- * Filter (3)
                     :     +- * ColumnarToRow (2)
                     :        +- Scan parquet  (1)
                     +- BroadcastQueryStage (9), Statistics(sizeInBytes=1026.1 KiB, rowCount=265)
                        +- BroadcastExchange (8)
                           +- * Project (7)
                              +- * Filter (6)
                                 +- Scan csv  (5)
+- == Initial Plan ==
   HashAggregate (26)
   +- Exchange (25)
      +- HashAggregate (24)
         +- Project (23)
            +- BroadcastHashJoin Inner BuildRight (22)
               :- Project (18)
               :  +- Filter (17)
               :     +- Scan parquet  (1)
               +- BroadcastExchange (21)
                  +- Project (20)
                     +- Filter (19)
                        +- Scan csv  (5)


(1) Scan parquet 
Output [4]: [pickup_location_id#194, total_amount#204, pickup_hour#207, pickup_date#218]
Batched: true
Location: InMemoryFileIndex [file:/Users/pedro/.gemini/antigravity/scratch/nyc-taxi-lakehouse/data/silver/trips]
PartitionFilters: [isnotnull(pickup_date#218), (pickup_date#218 >= 2024-01-15), (pickup_date#218 <= 2024-01-21)]
PushedFilters: [IsNotNull(pickup_location_id)]
ReadSchema: struct<pickup_location_id:int,total_amount:double,pickup_hour:int>

(2) ColumnarToRow [codegen id : 2]
Input [4]: [pickup_location_id#194, total_amount#204, pickup_hour#207, pickup_date#218]

(3) Filter [codegen id : 2]
Input [4]: [pickup_location_id#194, total_amount#204, pickup_hour#207, pickup_date#218]
Condition : isnotnull(pickup_location_id#194)

(4) Project [codegen id : 2]
Output [3]: [pickup_location_id#194, pickup_hour#207, total_amount#204]
Input [4]: [pickup_location_id#194, total_amount#204, pickup_hour#207, pickup_date#218]

(5) Scan csv 
Output [3]: [LocationID#266, Borough#267, service_zone#269]
Batched: false
Location: InMemoryFileIndex [file:/Users/pedro/.gemini/antigravity/scratch/nyc-taxi-lakehouse/data/bronze/zones/taxi_zone_lookup.csv]
PushedFilters: [IsNotNull(LocationID)]
ReadSchema: struct<LocationID:int,Borough:string,service_zone:string>

(6) Filter [codegen id : 1]
Input [3]: [LocationID#266, Borough#267, service_zone#269]
Condition : isnotnull(LocationID#266)

(7) Project [codegen id : 1]
Output [3]: [LocationID#266 AS pickup_location_id#274, Borough#267, service_zone#269]
Input [3]: [LocationID#266, Borough#267, service_zone#269]

(8) BroadcastExchange
Input [3]: [pickup_location_id#274, Borough#267, service_zone#269]
Arguments: HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=315]

(9) BroadcastQueryStage
Output [3]: [pickup_location_id#274, Borough#267, service_zone#269]
Arguments: 0

(10) BroadcastHashJoin [codegen id : 2]
Left keys [1]: [pickup_location_id#194]
Right keys [1]: [pickup_location_id#274]
Join type: Inner
Join condition: None

(11) Project [codegen id : 2]
Output [4]: [pickup_hour#207, total_amount#204, Borough#267, service_zone#269]
Input [6]: [pickup_location_id#194, pickup_hour#207, total_amount#204, pickup_location_id#274, Borough#267, service_zone#269]

(12) HashAggregate [codegen id : 2]
Input [4]: [pickup_hour#207, total_amount#204, Borough#267, service_zone#269]
Keys [3]: [Borough#267, service_zone#269, pickup_hour#207]
Functions [2]: [partial_count(1), partial_avg(total_amount#204)]
Aggregate Attributes [3]: [count#298L, sum#299, count#300L]
Results [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]

(13) Exchange
Input [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]
Arguments: hashpartitioning(Borough#267, service_zone#269, pickup_hour#207, 8), ENSURE_REQUIREMENTS, [plan_id=372]

(14) ShuffleQueryStage
Output [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]
Arguments: 1

(15) AQEShuffleRead
Input [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]
Arguments: coalesced

(16) HashAggregate [codegen id : 3]
Input [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]
Keys [3]: [Borough#267, service_zone#269, pickup_hour#207]
Functions [2]: [count(1), avg(total_amount#204)]
Aggregate Attributes [2]: [count(1)#289L, avg(total_amount#204)#291]
Results [5]: [Borough#267, service_zone#269, pickup_hour#207, count(1)#289L AS trips#290L, round(avg(total_amount#204)#291, 2) AS avg_amount#292]

(17) Filter
Input [4]: [pickup_location_id#194, total_amount#204, pickup_hour#207, pickup_date#218]
Condition : isnotnull(pickup_location_id#194)

(18) Project
Output [3]: [pickup_location_id#194, pickup_hour#207, total_amount#204]
Input [4]: [pickup_location_id#194, total_amount#204, pickup_hour#207, pickup_date#218]

(19) Filter
Input [3]: [LocationID#266, Borough#267, service_zone#269]
Condition : isnotnull(LocationID#266)

(20) Project
Output [3]: [LocationID#266 AS pickup_location_id#274, Borough#267, service_zone#269]
Input [3]: [LocationID#266, Borough#267, service_zone#269]

(21) BroadcastExchange
Input [3]: [pickup_location_id#274, Borough#267, service_zone#269]
Arguments: HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [plan_id=285]

(22) BroadcastHashJoin
Left keys [1]: [pickup_location_id#194]
Right keys [1]: [pickup_location_id#274]
Join type: Inner
Join condition: None

(23) Project
Output [4]: [pickup_hour#207, total_amount#204, Borough#267, service_zone#269]
Input [6]: [pickup_location_id#194, pickup_hour#207, total_amount#204, pickup_location_id#274, Borough#267, service_zone#269]

(24) HashAggregate
Input [4]: [pickup_hour#207, total_amount#204, Borough#267, service_zone#269]
Keys [3]: [Borough#267, service_zone#269, pickup_hour#207]
Functions [2]: [partial_count(1), partial_avg(total_amount#204)]
Aggregate Attributes [3]: [count#298L, sum#299, count#300L]
Results [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]

(25) Exchange
Input [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]
Arguments: hashpartitioning(Borough#267, service_zone#269, pickup_hour#207, 8), ENSURE_REQUIREMENTS, [plan_id=290]

(26) HashAggregate
Input [6]: [Borough#267, service_zone#269, pickup_hour#207, count#301L, sum#302, count#303L]
Keys [3]: [Borough#267, service_zone#269, pickup_hour#207]
Functions [2]: [count(1), avg(total_amount#204)]
Aggregate Attributes [2]: [count(1)#289L, avg(total_amount#204)#291]
Results [5]: [Borough#267, service_zone#269, pickup_hour#207, count(1)#289L AS trips#290L, round(avg(total_amount#204)#291, 2) AS avg_amount#292]

(27) AdaptiveSparkPlan
Output [5]: [Borough#267, service_zone#269, pickup_hour#207, trips#290L, avg_amount#292]
Arguments: isFinalPlan=true
```
