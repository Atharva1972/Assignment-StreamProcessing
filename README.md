# Hospital Patient Monitoring using PySpark

## Objective
Monitor patient heart rate data and generate alerts when the average heart rate exceeds 100 BPM.

## Technologies
- Python 3.14
- Apache Spark (PySpark 3.5.5)
- Java 17

## Dataset
Columns:
- patient_id
- heart_rate
- event_time

## Processing Logic
1. Load heart monitoring data.
2. Group events into 2-minute windows.
3. Calculate average heart rate.
4. Generate alerts when average heart rate > 100 BPM.

## Run

```bash
python streaming_job.py

## Implementation Note

The original design used Apache Spark Structured Streaming to monitor incoming patient heart-rate data in real time. During implementation on Windows, a Hadoop NativeIO compatibility issue was encountered:

java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0

Due to this platform-specific limitation, the solution was adapted to use batch processing while preserving the core analytics requirements:

* Schema-based data ingestion
* Timestamp processing
* 2-minute time window aggregation
* Average heart-rate computation
* Alert generation for average heart rate greater than 100 BPM

The implemented solution successfully demonstrates the required data processing and alerting logic using PySpark.

Windowing & State Management Explanation

## Why I Chose a 2-Minute Tumbling Window

I chose a 2 minute time window of tumbling as it’s a fixed period of time which is not overlapping and can be used to observe patient heart-rate data. In a hospital setting, healthcare professionals require timely detection of abnormal conditions, with a minimization of unnecessary alerts. A 2-minute window gives a good balance between response and stability as it combines several readings over a short period and smooths the effect of noisy individual readings.

For tumbling windows, each window contains exactly one event and the alerts that come out of the windows are easy to analyze and report on.

## Where the Pipeline Requires State

The windowed aggregation phase is a stage that needs state in the pipeline. For every patient, a 2 minute window has to store records at the same window but from different patients, so that Spark can calculate the average heart rate for each patient. This state will remain until the window's events have been processed and the average heart rate has been calculated.

The original streaming design also would need to include the state in the watermarking and late-arrival processing paths. Spark would store intermediate aggregate results in memory until it received a watermark from the processor that no more interesting events were likely on the way. In the final implementation, the same aggregation logic is used, asking for the state to group records and compute average heart rate before computing alerts for metric values greater than 100 beats per minute.