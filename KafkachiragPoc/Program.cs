using Confluent.Kafka;
using System.Text.Json;


var config = new ProducerConfig()
{
    BootstrapServers = "lkc-8w7qg0.dom8pmq03wy.eu-west-1.aws.confluent.cloud:9092",
    ClientId = "ClientGateway",
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "YAQVLKSLWXSKTBJ4",
    SaslPassword = "DRuMyHP//HlxcXFuvvk/H58/ntajZAFlbQQ1U4iaeJImlrX3RI0FqUt5PbFNTBEW",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    MessageTimeoutMs = 6000,
    Acks = Acks.None
};

using var producer =  new ProducerBuilder<Null,string>(config).Build();
try
{
    string? state;
    while ((state = Console.ReadLine()) != null)
    {
        var response = await producer.ProduceAsync("ndc_d_pii_callsign_scoringresponse", new Message<Null, string>
        {
            Value = JsonSerializer.Serialize(new Weather(state, 70))
        });
        Console.WriteLine(response.Value);
    }
    
}
catch (ProduceException<Null, string> exc)
{
    Console.WriteLine(exc.Message);
}

public record Weather(string State, int Temperature);