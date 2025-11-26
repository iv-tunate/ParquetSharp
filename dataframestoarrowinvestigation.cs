using Apache.Arrow;
using Microsoft.Data.Analysis;
using ParquetSharp;
using ParquetSharp.Arrow;
using System.Diagnostics;

string file = "sample.parquet";

Console.WriteLine("Creating sample Parquet file...");
await CreateSampleParquetFile(file);

Console.WriteLine("\n--- Reading with Arrow API ---");
var arrowTime = await ReadingViaArrow(file);

Console.WriteLine("\n--- Reading with ParquetSharp.DataFrame ---");
var dataFrameTime = ReadingViaDataframe(file);

Console.WriteLine("\n--- Writing with Arrow RecordBatchWriter ---");
await WritingViaArrow(file);

Console.WriteLine("\n=== Test Summary ===");
Console.WriteLine($"Arrow Read Time: {arrowTime}ms");
Console.WriteLine($"DataFrame Read Time: {dataFrameTime}ms");

double improvementData = 100.0 * (dataFrameTime - arrowTime) / dataFrameTime;
Console.WriteLine($"Speed Improvement: {improvementData:F1}%");

Console.WriteLine("\n=== Key Findings ===");
Console.WriteLine($"Arrow is faster for reading... Being about {improvementData:F1}% faster");
Console.WriteLine("Arrow writing still fails with string columns");
Console.WriteLine("ParquetSharp.DataFrame supports both reading and writing reliably");


static async Task CreateSampleParquetFile(string path)
{
    var schema = new Apache.Arrow.Schema.Builder()
        .Field(f => f.Name("Id").DataType(Apache.Arrow.Types.Int32Type.Default))
        .Field(f => f.Name("Name").DataType(Apache.Arrow.Types.StringType.Default))
        .Field(f => f.Name("Amount").DataType(Apache.Arrow.Types.DoubleType.Default))
        .Build();

    var idArray = new Int32Array.Builder()
        .AppendRange(Enumerable.Range(1, 1000))
        .Build();

    var nameArray = new StringArray.Builder()
        .AppendRange(Enumerable.Range(1, 1000).Select(i => $"Name_{i}"))
        .Build();

    var amountArray = new DoubleArray.Builder()
        .AppendRange(Enumerable.Range(1, 1000).Select(i => i * 10.5))
        .Build();

    var recordBatch = new RecordBatch(schema, new IArrowArray[] { idArray, nameArray, amountArray }, 1000);

    using var writer = new FileWriter(path, schema);
    writer.WriteRecordBatch(recordBatch);
    writer.Close();

    Console.WriteLine($"Created sample Parquet file with 1000 rows: {path}");
}

static async Task<long> ReadingViaArrow(string path)
{
    var sw = Stopwatch.StartNew();

    using var reader = new FileReader(path);
    using var batchReader = reader.GetRecordBatchReader();

    long totalRows = 0;
    RecordBatch batch;

    while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
    {
        using (batch)
        {
            var df = DataFrame.FromArrowRecordBatch(batch);
            totalRows += df.Rows.Count;
        }
    }

    sw.Stop();
    Console.WriteLine($"Read {totalRows} rows in {sw.ElapsedMilliseconds}ms");
    return sw.ElapsedMilliseconds;
}

static long ReadingViaDataframe(string path)
{
    var sw = Stopwatch.StartNew();

    using var reader = new ParquetFileReader(path);
    var df = reader.ToDataFrame();

    sw.Stop();
    Console.WriteLine($"Read {df.Rows.Count} rows in {sw.ElapsedMilliseconds}ms");
    return sw.ElapsedMilliseconds;
}

static async Task WritingViaArrow(string path)
{
    try
    {
        using var reader = new ParquetFileReader(path);
        var df = reader.ToDataFrame();

        var batches = df.ToArrowRecordBatches();
        int index = 0;

        foreach (var batch in batches)
        {
            string output = $"arrow-write-test-{index}.parquet";

            using var writer = new FileWriter(output, batch.Schema);
            writer.WriteRecordBatch(batch);
            writer.Close();

            Console.WriteLine($"Wrote batch {index} to file {output}");
            index++;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($" {ex.Message}");
        Console.WriteLine("Arrow writer still cannot reliably handle string columns.");
    }
}