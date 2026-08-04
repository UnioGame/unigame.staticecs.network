using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace UniGame.StaticEcs.Network.Generator
{
    [Generator]
    public sealed class NetworkSourceGenerator : IIncrementalGenerator
    {
        private const string NetworkType = "UniGame.StaticEcs.Network.INetworkType";
        private const string NetworkCommand = "UniGame.StaticEcs.Network.INetworkCommand";
        private const string EndpointAttribute = "UniGame.StaticEcs.Network.NetworkEndpointAttribute";
        private const string ManifestRecordAttribute = "UniGame.StaticEcs.Network.NetworkManifestRecordAttribute";
        private static readonly DiagnosticDescriptor InvalidWireType = Error("NETV2001", "Invalid network wire type", "Network wire type '{0}' must be a concrete non-generic struct");
        private static readonly DiagnosticDescriptor Collision = Error("NETV2002", "Network type id collision", "Network type id 0x{0:x8} collides between '{1}' and '{2}'");
        private static readonly DiagnosticDescriptor InvalidShape = Error("NETV2003", "Ambiguous Static ECS shape", "Network wire type '{0}' must implement exactly one supported Static ECS shape");
        private static readonly DiagnosticDescriptor InvalidEndpoint = Error("NETV2004", "Invalid network endpoint", "Endpoint name '{0}' must be a unique C# identifier in this compilation");
        private static readonly DiagnosticDescriptor EndpointWorld = Error("NETV2005", "Invalid endpoint world", "Endpoint '{0}' must reference a concrete struct IWorldType");

        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            var candidates = context.SyntaxProvider.CreateSyntaxProvider(
                static (node, _) => node is StructDeclarationSyntax,
                static (syntax, _) => ((StructDeclarationSyntax)syntax.Node).Identifier.ValueText)
                .Collect();
            var input = context.CompilationProvider.Combine(candidates);
            context.RegisterSourceOutput(input, static (production, value) => Execute(value.Left, production));
        }

        private static void Execute(Compilation compilation, SourceProductionContext context)
        {
            if (compilation.GetTypeByMetadataName(NetworkType) == null ||
                compilation.GetTypeByMetadataName("UniGame.StaticEcs.Network.NetworkCompilerSupport") == null) return;
            var records = new List<Record>();
            CollectTypes(compilation.Assembly.GlobalNamespace, compilation.AssemblyName ?? string.Empty, records, context);
            var referenced = CollectReferenced(compilation);
            ValidateCollisions(records.Concat(referenced), context);
            EmitManifest(records, context);
            EmitEndpoints(compilation, records, referenced, context);
        }

        private static void CollectTypes(INamespaceSymbol ns, string assemblyName, List<Record> records, SourceProductionContext context)
        {
            foreach (var child in ns.GetNamespaceMembers()) CollectTypes(child, assemblyName, records, context);
            foreach (var type in ns.GetTypeMembers()) CollectType(type, assemblyName, records, context);
        }

        private static void CollectType(INamedTypeSymbol type, string assemblyName, List<Record> records, SourceProductionContext context)
        {
            foreach (var nested in type.GetTypeMembers()) CollectType(nested, assemblyName, records, context);
            var isType = Implements(type, NetworkType);
            var isCommand = Implements(type, NetworkCommand);
            if (!isType && !isCommand) return;
            if (type.TypeKind != TypeKind.Struct || type.IsGenericType || type.IsAbstract)
            {
                context.ReportDiagnostic(Diagnostic.Create(InvalidWireType, type.Locations.FirstOrDefault(), type.ToDisplayString()));
                return;
            }
            var shapes = new List<Kind>();
            if (Implements(type, "FFS.Libraries.StaticEcs.IEntityType")) shapes.Add(Kind.Entity);
            if (Implements(type, "FFS.Libraries.StaticEcs.IComponent")) shapes.Add(Kind.Component);
            if (Implements(type, "FFS.Libraries.StaticEcs.ITag")) shapes.Add(Kind.Tag);
            if (Implements(type, "FFS.Libraries.StaticEcs.ILinksType")) shapes.Add(Kind.Links);
            else if (Implements(type, "FFS.Libraries.StaticEcs.ILinkType")) shapes.Add(Kind.Link);
            if (Implements(type, "FFS.Libraries.StaticEcs.IMultiComponent")) shapes.Add(Kind.Multi);
            if (isCommand && Implements(type, "FFS.Libraries.StaticEcs.IEvent")) shapes.Add(Kind.Command);
            if (shapes.Count != 1)
            {
                context.ReportDiagnostic(Diagnostic.Create(InvalidShape, type.Locations.FirstOrDefault(), type.ToDisplayString()));
                return;
            }
            var metadataName = MetadataName(type);
            records.Add(new Record(Hash(Encoding.UTF8.GetBytes(assemblyName + ":" + metadataName)), shapes[0], type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), metadataName, 1));
        }

        private static List<Record> CollectReferenced(Compilation compilation)
        {
            var records = new List<Record>();
            foreach (var reference in compilation.SourceModule.ReferencedAssemblySymbols)
            foreach (var attribute in reference.GetAttributes())
            {
                if (attribute.AttributeClass?.ToDisplayString() != ManifestRecordAttribute || attribute.ConstructorArguments.Length < 3) continue;
                var id = (uint)attribute.ConstructorArguments[0].Value;
                var kind = (Kind)Convert.ToInt32(attribute.ConstructorArguments[1].Value);
                var type = attribute.ConstructorArguments[2].Value as INamedTypeSymbol;
                var version = attribute.ConstructorArguments.Length > 3 ? (byte)attribute.ConstructorArguments[3].Value : (byte)1;
                if (type != null) records.Add(new Record(id, kind, type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), MetadataName(type), version));
            }
            return records;
        }

        private static void ValidateCollisions(IEnumerable<Record> source, SourceProductionContext context)
        {
            var ids = new Dictionary<uint, Record>();
            foreach (var record in source)
            {
                if (!ids.TryGetValue(record.Id, out var previous)) { ids.Add(record.Id, record); continue; }
                if (previous.TypeName != record.TypeName) context.ReportDiagnostic(Diagnostic.Create(Collision, Location.None, record.Id, previous.MetadataName, record.MetadataName));
            }
        }

        private static void EmitManifest(List<Record> records, SourceProductionContext context)
        {
            var source = new StringBuilder("// <auto-generated/>\n");
            foreach (var record in records.OrderBy(r => r.Kind).ThenBy(r => r.Id))
                source.Append("[assembly: global::UniGame.StaticEcs.Network.NetworkManifestRecordAttribute(").Append(record.Id).Append("u, global::UniGame.StaticEcs.Network.NetworkSchemaKind.").Append(record.Kind).Append(", typeof(").Append(record.TypeName).Append("), ").Append(record.Version).Append(")]\n");
            source.Append("[global::UniGame.StaticEcs.Network.NetworkManifestAttribute]\npublic static class __GeneratedNetworkManifest\n{\n")
                .Append("    [global::System.ComponentModel.EditorBrowsable(global::System.ComponentModel.EditorBrowsableState.Never)]\n")
                .Append("    public static void Append<TWorld>(global::UniGame.StaticEcs.Network.NetworkCompilerSchemaFactory<TWorld> factory) where TWorld : struct, global::FFS.Libraries.StaticEcs.IWorldType\n    {\n");
            foreach (var record in records.OrderBy(r => r.Kind).ThenBy(r => r.Id)) AppendRegistration(source, record, "factory");
            source.Append("    }\n}\n");
            context.AddSource("NetworkManifest.g.cs", SourceText.From(source.ToString(), Encoding.UTF8));
        }

        private static void EmitEndpoints(Compilation compilation, List<Record> local, List<Record> referenced, SourceProductionContext context)
        {
            var endpoints = compilation.Assembly.GetAttributes().Where(a => a.AttributeClass?.ToDisplayString() == EndpointAttribute).ToArray();
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var endpoint in endpoints)
            {
                if (endpoint.ConstructorArguments.Length != 3) continue;
                var name = endpoint.ConstructorArguments[0].Value as string ?? string.Empty;
                var world = endpoint.ConstructorArguments[1].Value as INamedTypeSymbol;
                var role = Convert.ToInt32(endpoint.ConstructorArguments[2].Value);
                if (!Microsoft.CodeAnalysis.CSharp.SyntaxFacts.IsValidIdentifier(name) || !names.Add(name)) { context.ReportDiagnostic(Diagnostic.Create(InvalidEndpoint, Location.None, name)); continue; }
                if (world == null || world.TypeKind != TypeKind.Struct || !Implements(world, "FFS.Libraries.StaticEcs.IWorldType")) { context.ReportDiagnostic(Diagnostic.Create(EndpointWorld, Location.None, name)); continue; }
                var source = new StringBuilder("// <auto-generated/>\npublic static class Generated").Append(name).Append("Network\n{\n");
                var worldName = world.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                source.Append("    public static global::UniGame.StaticEcs.Network.NetworkSchema<").Append(worldName).Append("> CreateSchema()\n    {\n")
                    .Append("        var factory = global::UniGame.StaticEcs.Network.NetworkCompilerSupport.Create<").Append(worldName).Append(">();\n");
                foreach (var record in local.Concat(referenced).GroupBy(r => r.TypeName).Select(g => g.First()).OrderBy(r => r.Kind).ThenBy(r => r.Id)) AppendRegistration(source, record, "factory", 8);
                if (role == 2) AppendPolicies(compilation.Assembly.GlobalNamespace, world, source);
                source.Append("        return factory.Freeze();\n    }\n")
                    .Append("    public static void RegisterTypes(global::FFS.Libraries.StaticEcs.World<").Append(worldName).Append(">.TypeRegistrar registrar)\n    {\n");
                if (role == 2) AppendPolicyEvents(compilation.Assembly.GlobalNamespace, world, source);
                source.Append("    }\n}\n");
                context.AddSource("Generated" + name + "Network.g.cs", SourceText.From(source.ToString(), Encoding.UTF8));
            }
        }

        private static void AppendPolicies(INamespaceSymbol ns, INamedTypeSymbol world, StringBuilder source)
        {
            foreach (var child in ns.GetNamespaceMembers()) AppendPolicies(child, world, source);
            foreach (var type in ns.GetTypeMembers())
            foreach (var iface in type.AllInterfaces)
                if (iface.OriginalDefinition.ToDisplayString() == "UniGame.StaticEcs.Network.INetworkCommandPolicy<TWorld, TCommand>" && SymbolEqualityComparer.Default.Equals(iface.TypeArguments[0], world))
                    source.Append("        factory.Policy<").Append(iface.TypeArguments[1].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)).Append(", ").Append(type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)).Append(">();\n");
        }

        private static void AppendPolicyEvents(INamespaceSymbol ns, INamedTypeSymbol world, StringBuilder source)
        {
            foreach (var child in ns.GetNamespaceMembers()) AppendPolicyEvents(child, world, source);
            foreach (var type in ns.GetTypeMembers())
            foreach (var iface in type.AllInterfaces)
                if (iface.OriginalDefinition.ToDisplayString() == "UniGame.StaticEcs.Network.INetworkCommandPolicy<TWorld, TCommand>" && SymbolEqualityComparer.Default.Equals(iface.TypeArguments[0], world))
                {
                    var command = iface.TypeArguments[1].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    source.Append("        registrar.Event<global::UniGame.StaticEcs.Network.NetworkCommandAccepted<").Append(command).Append(">>();\n")
                        .Append("        registrar.Event<global::UniGame.StaticEcs.Network.NetworkCommandRejected<").Append(command).Append(">>();\n");
                }
        }

        private static void AppendRegistration(StringBuilder source, Record record, string factory, int indent = 8)
        {
            source.Append(' ', indent).Append(factory).Append('.').Append(record.Kind).Append('<').Append(record.TypeName).Append(">(new global::UniGame.StaticEcs.Network.NetworkTypeId(").Append(record.Id).Append("u)");
            if (record.Kind != Kind.Entity) source.Append(", ").Append(record.Version);
            source.Append(");\n");
        }

        private static bool Implements(INamedTypeSymbol type, string metadataName) => type.AllInterfaces.Any(i => i.ToDisplayString() == metadataName);
        private static string MetadataName(INamedTypeSymbol type)
        {
            var names = new Stack<string>();
            for (var current = type; current != null; current = current.ContainingType) names.Push(current.MetadataName);
            var prefix = type.ContainingNamespace.IsGlobalNamespace ? string.Empty : type.ContainingNamespace.ToDisplayString() + ".";
            return prefix + string.Join("+", names);
        }
        private static DiagnosticDescriptor Error(string id, string title, string message) => new DiagnosticDescriptor(id, title, message, "StaticEcs.Network", DiagnosticSeverity.Error, true);
        private static uint Hash(byte[] data)
        {
            const uint p1 = 2654435761U, p2 = 2246822519U, p3 = 3266489917U, p4 = 668265263U, p5 = 374761393U;
            var index = 0; uint hash;
            if (data.Length >= 16)
            {
                var v1 = unchecked(p1 + p2); var v2 = p2; var v3 = 0U; var v4 = unchecked(0U - p1); var limit = data.Length - 16;
                do { v1 = Rot(unchecked(v1 + Read(data, index) * p2), 13) * p1; index += 4; v2 = Rot(unchecked(v2 + Read(data, index) * p2), 13) * p1; index += 4; v3 = Rot(unchecked(v3 + Read(data, index) * p2), 13) * p1; index += 4; v4 = Rot(unchecked(v4 + Read(data, index) * p2), 13) * p1; index += 4; } while (index <= limit);
                hash = Rot(v1, 1) + Rot(v2, 7) + Rot(v3, 12) + Rot(v4, 18);
            }
            else hash = p5;
            hash += (uint)data.Length;
            while (index <= data.Length - 4) { hash = Rot(unchecked(hash + Read(data, index) * p3), 17) * p4; index += 4; }
            while (index < data.Length) { hash = Rot(unchecked(hash + data[index] * p5), 11) * p1; index++; }
            hash ^= hash >> 15; hash *= p2; hash ^= hash >> 13; hash *= p3; hash ^= hash >> 16;
            return hash;
        }
        private static uint Read(byte[] data, int i) => (uint)(data[i] | data[i + 1] << 8 | data[i + 2] << 16 | data[i + 3] << 24);
        private static uint Rot(uint value, int bits) => value << bits | value >> (32 - bits);
        private enum Kind { Entity, Component, Tag, Link, Links, Multi, Command }
        private readonly struct Record
        {
            internal Record(uint id, Kind kind, string typeName, string metadataName, byte version) { Id = id; Kind = kind; TypeName = typeName; MetadataName = metadataName; Version = version; }
            internal uint Id { get; } internal Kind Kind { get; } internal string TypeName { get; } internal string MetadataName { get; } internal byte Version { get; }
        }
    }
}
