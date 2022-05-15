using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using System.Text;
using System.Text.Json;

const string stream_name = "hello-kinesis";

RegionEndpoint region = RegionEndpoint.USWest2;

string source = "producer1";
int count = 1;
int delay = 1;
string message = null!;

// Parse command line. Expected:
// dotnet run -- <message>
// dotnet run -- <producer-id> <count> <delay> <message>

if (args.Length==1)
{
    message = args[0];
}
else if (args.Length==4)
{
    source = args[0];
    count = Convert.ToInt32(args[1]);
    delay = Convert.ToInt32(args[2]);
    message = args[3];
}
else
{
    Console.WriteLine($"This command will send data records to Kinesis data stream {stream_name}");
    Console.WriteLine(@"To send a single message   dotnet run -- ""<message>""");
    Console.WriteLine(@"To send multiple messages: dotnet run -- <source-name> <count> <delay-in-seconds> ""<message>""");
    Environment.Exit(0);
}

var client = new AmazonKinesisClient(region);

// Create data record and serialize to UTF8-encoded bytes

var id = Guid.NewGuid();
var data = new { Id = id, Source = source, Message = message, Timestamp = DateTime.Now.ToString() };
byte[] dataBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));

// Create a memory stream from data record and put to the Kinesis stream

using (MemoryStream ms = new MemoryStream(dataBytes))
{
    var request = new PutRecordRequest()
    {
        StreamName = stream_name,
        PartitionKey = source,
        Data = ms
    };

    Console.WriteLine($"Writing as source {source} to Kinesis stream {stream_name}");

    for (int i = 0; i < count; i++)
    {
        var response = await client.PutRecordAsync(request);
        Console.WriteLine($"{data.Timestamp} sequence number {response.SequenceNumber}: shard Id: {response.ShardId}");
        Thread.Sleep(delay * 1000);
    }
}
