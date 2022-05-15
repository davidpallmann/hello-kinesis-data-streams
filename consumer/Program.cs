using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using System.Text;

class Program
{
    const string stream_name = "hello-kinesis";
    static RegionEndpoint region = RegionEndpoint.USWest2;

    static AmazonKinesisClient _client = null!;

    public static async Task Main(string[] args)
    {
        _client = new AmazonKinesisClient(region);

        // Describe stream to get list of shards

        var describeRequest = new DescribeStreamRequest()
        {
            StreamName = stream_name
        };
        var describeResponse = await _client.DescribeStreamAsync(describeRequest);
        List<Shard> shards = describeResponse.StreamDescription.Shards;

        Console.WriteLine($"Listing records for Kinesis stream {stream_name} - interrupt program to stop.");
        Console.WriteLine();

        // Spawn a thread for each shard

        var threads = new List<Thread>();
        foreach (var shard in shards)
        {
            var thread = new Thread(MonitorShard);
            thread.Start(shard.ShardId);
            threads.Add(thread);
            Console.WriteLine($"Started thread to monitor shard {shard.ShardId}");
        }

        Console.ReadLine();

    }

    // Monitor shard method (signature needed for Thread.Start)

    private static async void MonitorShard(object? shard) => await MonitorShard((string)shard!);

    // Monitor shard methid (implementation)

    private static async Task MonitorShard(string shard)
    {
        // Get iterator for shard

        var iteratorRequest = new GetShardIteratorRequest()
        {
            StreamName = stream_name,
            ShardId = shard,
            ShardIteratorType = ShardIteratorType.TRIM_HORIZON
        };

        // Retrieve and display records for shard

        var iteratorResponse = await _client.GetShardIteratorAsync(iteratorRequest);
        string iterator = iteratorResponse.ShardIterator;

        while (iterator != null)
        {
            // Get records from iterator

            var getRequest = new GetRecordsRequest()
            {
                Limit = 100,
                ShardIterator = iterator
            };

            var getResponse = await _client.GetRecordsAsync(getRequest);
            var records = getResponse.Records;

            // Display records

            if (records.Count > 0)
            {
                foreach (var record in records)
                {
                    var recordDisplay = Encoding.UTF8.GetString(record.Data.ToArray());
                    Console.WriteLine($"Record: {recordDisplay}, Partition Key: {record.PartitionKey}, Shard: {shard}");
                }
            }

            // Get next iterator

            iterator = getResponse.NextShardIterator;
            Thread.Sleep(100);
        }
    }
}


