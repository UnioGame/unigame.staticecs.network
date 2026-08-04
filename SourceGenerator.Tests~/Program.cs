using System;

internal static class Program
{
    public static int Main()
    {
        var client = GeneratedClientNetwork.CreateSchema();
        var server = GeneratedServerNetwork.CreateSchema();
        if (client.Fingerprint != server.Fingerprint) throw new InvalidOperationException("Generated Client and Server schemas differ.");
        if (client.Versions.Length != 2 || client.Versions[0] != 7 && client.Versions[0] != 8 || client.Versions[1] != 9) throw new InvalidOperationException("Generated config versions were not executed.");
        Console.WriteLine("SCHEMA:" + client.Fingerprint);
        return 0;
    }
}
