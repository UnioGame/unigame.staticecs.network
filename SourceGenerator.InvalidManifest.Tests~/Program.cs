using System;

internal static class Program
{
    public static int Main()
    {
        Console.WriteLine("SCHEMA:" + GeneratedServerNetwork.CreateSchema().Fingerprint);
        return 0;
    }
}
