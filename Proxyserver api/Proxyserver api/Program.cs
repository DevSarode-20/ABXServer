using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

class Packet
{
    public string Symbol { get; set; }
    public char BuySell { get; set; }
    public int Quantity { get; set; }
    public int Price { get; set; }
    public int Sequence { get; set; }
}

class AbxClient
{
    const int Port = 3000;
    const string Host = "127.0.0.1";  
    const int PacketSize = 17;

    public List<Packet> RequestStreamAllPackets()
    {
        var packets = new Dictionary<int, Packet>();

        using TcpClient client = new TcpClient(Host, Port);
        using NetworkStream stream = client.GetStream();

        byte[] requestPayload = new byte[2] { 1, 0 };
        stream.Write(requestPayload, 0, requestPayload.Length);
        stream.Flush();

        while (true)
        {
            byte[] buffer = new byte[PacketSize];
            int bytesRead = 0;
            while (bytesRead < PacketSize)
            {
                int read = stream.Read(buffer, bytesRead, PacketSize - bytesRead);
                if (read == 0)
                {
                    break;
                }
                bytesRead += read;
            }
            if (bytesRead < PacketSize)
            {
                break;  
            }

            Packet packet = ParsePacket(buffer);
            packets[packet.Sequence] = packet;
        }
        var sortedPackets = new List<Packet>(packets.Values);
        sortedPackets.Sort((a, b) => a.Sequence.CompareTo(b.Sequence));
        return sortedPackets;
    }

    public Packet RequestResendPacket(int sequence)
    {
        using TcpClient client = new TcpClient(Host, Port);
        using NetworkStream stream = client.GetStream();

        byte[] requestPayload = new byte[2] { 2, (byte)sequence };
        stream.Write(requestPayload, 0, requestPayload.Length);
        stream.Flush();

        byte[] buffer = new byte[PacketSize];
        int bytesRead = 0;
        while (bytesRead < PacketSize)
        {
            int read = stream.Read(buffer, bytesRead, PacketSize - bytesRead);
            if (read == 0)
                throw new Exception("Connection closed unexpectedly during resend packet");
            bytesRead += read;
        }

        return ParsePacket(buffer);
    }

    private Packet ParsePacket(byte[] buffer)
    {
    
        string symbol = Encoding.ASCII.GetString(buffer, 0, 4);
        char buySell = (char)buffer[4];

        int quantity = (buffer[5] << 24) | (buffer[6] << 16) | (buffer[7] << 8) | buffer[8];
        int price = (buffer[9] << 24) | (buffer[10] << 16) | (buffer[11] << 8) | buffer[12];
        int sequence = (buffer[13] << 24) | (buffer[14] << 16) | (buffer[15] << 8) | buffer[16];

        return new Packet
        {
            Symbol = symbol,
            BuySell = buySell,
            Quantity = quantity,
            Price = price,
            Sequence = sequence
        };
    }

    public void RunAndSaveJson(string outputPath)
    {
        Console.WriteLine("Requesting all packets stream...");
        var packets = RequestStreamAllPackets();

        if (packets.Count == 0)
        {
            Console.WriteLine("No packets received.");
            return;
        }

        Console.WriteLine($"Received {packets.Count} packets.");
        int minSeq = packets[0].Sequence;
        int maxSeq = packets[packets.Count - 1].Sequence;

        var existingSequences = new HashSet<int>();
        foreach (var p in packets) existingSequences.Add(p.Sequence);

        var missingSequences = new List<int>();
        for (int seq = minSeq; seq < maxSeq; seq++)
        {
            if (!existingSequences.Contains(seq))
                missingSequences.Add(seq);
        }

        Console.WriteLine($"Missing sequences: {string.Join(", ", missingSequences)}");
        foreach (int seq in missingSequences)
        {
            Console.WriteLine($"Requesting missing packet seq={seq}...");
            Packet missingPacket = RequestResendPacket(seq);
            packets.Add(missingPacket);
        }

        packets.Sort((a, b) => a.Sequence.CompareTo(b.Sequence));
       var options = new JsonSerializerOptions { WriteIndented = true };
        string json = JsonSerializer.Serialize(packets, options);
        File.WriteAllText(outputPath, json);
        Console.WriteLine($"JSON output saved to {outputPath}");
    }
}

class Program
{
    static void Main()
    {
        var client = new AbxClient();
        client.RunAndSaveJson("abx_packets.json");
    }
}
