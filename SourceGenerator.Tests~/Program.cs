using System;

internal static class Program
{
    public static int Main()
    {
        var client = GeneratedClientNetwork.CreateSchema();
        var server = GeneratedServerNetwork.CreateSchema();
        if (client.Fingerprint != server.Fingerprint) throw new InvalidOperationException("Generated Client and Server schemas differ.");
        var defaultVersions = 0;
        for (var i = 0; i < client.Versions.Length; i++) if (client.Versions[i] == 0) defaultVersions++;
        if (client.Versions.Length != 5 || defaultVersions != 3 ||
            Array.IndexOf(client.Versions, (byte)9) < 0 || Array.IndexOf(client.Versions, (byte)7) < 0 &&
            Array.IndexOf(client.Versions, (byte)8) < 0)
            throw new InvalidOperationException("Generated configured and default versions were not executed.");
        Console.WriteLine("SCHEMA:" + client.Fingerprint);
        return 0;
    }
}
