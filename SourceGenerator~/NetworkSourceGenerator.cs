using System;
using System.Collections.Generic;
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
        private static readonly DiagnosticDescriptor SharedOnly = Error("NETV2006", "Wire type outside Shared assembly", "Network wire type '{0}' cannot be declared in an endpoint assembly");
        private static readonly DiagnosticDescriptor MissingHooks = Error("NETV2007", "Missing serialization hooks", "Network wire type '{0}' does not expose the required Static ECS Write and Read hooks");
        private static readonly DiagnosticDescriptor ZeroId = Error("NETV2008", "Zero network type id", "Network wire type '{0}' generated the reserved zero id");
        private static readonly DiagnosticDescriptor MissingPolicy = Error("NETV2009", "Missing server command policy", "Server endpoint '{0}' must have exactly one reachable policy for command '{1}'");
        private static readonly DiagnosticDescriptor DuplicatePolicy = Error("NETV2010", "Duplicate server command policy", "Server endpoint '{0}' has multiple reachable policies for command '{1}'");
        private static readonly DiagnosticDescriptor UnknownPolicy = Error("NETV2011", "Policy command is not reachable", "Server policy '{0}' targets command '{1}' which is absent from the aggregated Shared manifest");

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
            var hasEndpoints = compilation.Assembly.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == EndpointAttribute);
            if (records.Count == 0 && !hasEndpoints) return;
            var referenced = CollectReferenced(compilation, context);
            if (hasEndpoints) foreach (var record in records) context.ReportDiagnostic(Diagnostic.Create(SharedOnly, record.Symbol.Locations.FirstOrDefault(), record.Symbol.ToDisplayString()));
            ValidateCollisions(records, context);
            EmitManifest(records, context);
            EmitEndpoints(compilation, referenced, context);
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
            if (!HasRequiredHooks(type, shapes[0]))
            {
                context.ReportDiagnostic(Diagnostic.Create(MissingHooks, type.Locations.FirstOrDefault(), type.ToDisplayString()));
                return;
            }
            var metadataName = MetadataName(type);
            var id = Hash(Encoding.UTF8.GetBytes(assemblyName + ":" + metadataName));
            if (id == 0) { context.ReportDiagnostic(Diagnostic.Create(ZeroId, type.Locations.FirstOrDefault(), type.ToDisplayString())); return; }
            records.Add(new Record(id, shapes[0], type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), metadataName, 0, type, type.ContainingAssembly));
        }

        private static List<Record> CollectReferenced(Compilation compilation, SourceProductionContext context)
        {
            var records = new List<Record>();
            foreach (var reference in compilation.SourceModule.ReferencedAssemblySymbols)
            foreach (var attribute in reference.GetAttributes())
            {
                if (attribute.AttributeClass?.ToDisplayString() != ManifestRecordAttribute || attribute.ConstructorArguments.Length < 3) continue;
                var id = (uint)attribute.ConstructorArguments[0].Value;
                var kind = (Kind)Convert.ToInt32(attribute.ConstructorArguments[1].Value);
                var type = attribute.ConstructorArguments[2].Value as INamedTypeSymbol;
                var version = attribute.ConstructorArguments.Length > 3 ? (byte)attribute.ConstructorArguments[3].Value : (byte)0;
                if (type != null) records.Add(new Record(id, kind, type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), MetadataName(type), version, type, reference));
            }
            return records;
        }

        private static List<Record> SelectReferenced(List<Record> referenced, AttributeData endpoint)
        {
            var assemblies = new List<IAssemblySymbol>();
            if (endpoint.ConstructorArguments.Length > 3)
            {
                var roots = endpoint.ConstructorArguments[3];
                if (roots.Kind == TypedConstantKind.Array)
                    foreach (var root in roots.Values)
                        if (root.Value is INamedTypeSymbol symbol) AddRootAssembly(assemblies, symbol);
                else if (roots.Value is INamedTypeSymbol singleRoot) AddRootAssembly(assemblies, singleRoot);
            }
            var records = new List<Record>();
            foreach (var record in referenced)
                if (record.OriginAssembly != null && assemblies.Any(assembly => SymbolEqualityComparer.Default.Equals(assembly, record.OriginAssembly)))
                    records.Add(record);
            return records;
        }

        private static void AddRootAssembly(List<IAssemblySymbol> assemblies, INamedTypeSymbol root)
        {
            if (root.ContainingAssembly == null || assemblies.Any(assembly => SymbolEqualityComparer.Default.Equals(assembly, root.ContainingAssembly))) return;
            assemblies.Add(root.ContainingAssembly);
        }

        private static void ReportZeroIds(IEnumerable<Record> records, SourceProductionContext context)
        {
            var reported = new HashSet<string>(StringComparer.Ordinal);
            foreach (var record in records)
                if (record.Id == 0 && reported.Add(record.TypeName))
                    context.ReportDiagnostic(Diagnostic.Create(ZeroId, record.Symbol.Locations.FirstOrDefault(), record.TypeName));
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

        private static void EmitEndpoints(Compilation compilation, List<Record> referenced, SourceProductionContext context)
        {
            var endpoints = compilation.Assembly.GetAttributes().Where(a => a.AttributeClass?.ToDisplayString() == EndpointAttribute).ToArray();
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var endpoint in endpoints)
            {
                if (endpoint.ConstructorArguments.Length < 3) continue;
                var name = endpoint.ConstructorArguments[0].Value as string ?? string.Empty;
                var world = endpoint.ConstructorArguments[1].Value as INamedTypeSymbol;
                var role = Convert.ToInt32(endpoint.ConstructorArguments[2].Value);
                if (!Microsoft.CodeAnalysis.CSharp.SyntaxFacts.IsValidIdentifier(name) || !names.Add(name)) { context.ReportDiagnostic(Diagnostic.Create(InvalidEndpoint, Location.None, name)); continue; }
                if (world == null || world.TypeKind != TypeKind.Struct || !Implements(world, "FFS.Libraries.StaticEcs.IWorldType")) { context.ReportDiagnostic(Diagnostic.Create(EndpointWorld, Location.None, name)); continue; }
                var selected = SelectReferenced(referenced, endpoint);
                ReportZeroIds(selected, context);
                selected.RemoveAll(record => record.Id == 0);
                ValidateCollisions(selected, context);
                var source = new StringBuilder("// <auto-generated/>\npublic static class Generated").Append(name).Append("Network\n{\n");
                var worldName = world.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                source.Append("    public static global::UniGame.StaticEcs.Network.NetworkSchema<").Append(worldName).Append("> CreateSchema()\n    {\n")
                    .Append("        var factory = global::UniGame.StaticEcs.Network.NetworkCompilerSupport.Create<").Append(worldName).Append(">();\n");
                foreach (var record in selected.GroupBy(r => r.TypeName).Select(g => g.First()).OrderBy(r => r.Kind).ThenBy(r => r.Id)) AppendRegistration(source, record, "factory", 8);
                if (role == 2)
                {
                    var commands = selected.Where(r => r.Kind == Kind.Command).GroupBy(r => r.TypeName).Select(g => g.First()).ToArray();
                    var policies = CollectPolicies(compilation.Assembly.GlobalNamespace, world);
                    foreach (var command in commands)
                    {
                        var matches = policies.Where(p => p.CommandName == command.TypeName).ToArray();
                        if (matches.Length == 0) context.ReportDiagnostic(Diagnostic.Create(MissingPolicy, Location.None, name, command.MetadataName));
                        else if (matches.Length > 1) context.ReportDiagnostic(Diagnostic.Create(DuplicatePolicy, Location.None, name, command.MetadataName));
                    }
                    foreach (var policy in policies) if (!commands.Any(c => c.TypeName == policy.CommandName)) context.ReportDiagnostic(Diagnostic.Create(UnknownPolicy, policy.Type.Locations.FirstOrDefault(), policy.Type.ToDisplayString(), policy.CommandName));
                    AppendPolicies(policies.Where(policy => commands.Any(command => command.TypeName == policy.CommandName)), source);
                }
                source.Append("        return factory.Freeze();\n    }\n")
                    .Append("    public static void RegisterTypes(global::FFS.Libraries.StaticEcs.World<").Append(worldName).Append(">.TypeRegistrar registrar)\n    {\n");
                if (role == 2)
                {
                    var commands = selected.Where(record => record.Kind == Kind.Command).Select(record => record.TypeName).ToArray();
                    AppendPolicyEvents(CollectPolicies(compilation.Assembly.GlobalNamespace, world)
                        .Where(policy => commands.Contains(policy.CommandName)), source);
                }
                source.Append("    }\n}\n");
                context.AddSource("Generated" + name + "Network.g.cs", SourceText.From(source.ToString(), Encoding.UTF8));
            }
        }

        private static List<Policy> CollectPolicies(INamespaceSymbol ns, INamedTypeSymbol world)
        {
            var policies = new List<Policy>();
            CollectPolicies(ns, world, policies);
            return policies;
        }

        private static void CollectPolicies(INamespaceSymbol ns, INamedTypeSymbol world, List<Policy> policies)
        {
            foreach (var child in ns.GetNamespaceMembers()) CollectPolicies(child, world, policies);
            foreach (var type in ns.GetTypeMembers())
            foreach (var iface in type.AllInterfaces)
                if (iface.OriginalDefinition.ToDisplayString() == "UniGame.StaticEcs.Network.INetworkCommandPolicy<TWorld, TCommand>" && SymbolEqualityComparer.Default.Equals(iface.TypeArguments[0], world))
                    policies.Add(new Policy(type, iface.TypeArguments[1].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)));
        }

        private static void AppendPolicies(IEnumerable<Policy> policies, StringBuilder source)
        {
            foreach (var policy in policies) source.Append("        factory.Policy<").Append(policy.CommandName).Append(", ").Append(policy.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)).Append(">();\n");
        }

        private static void AppendPolicyEvents(IEnumerable<Policy> policies, StringBuilder source)
        {
            foreach (var command in policies.Select(p => p.CommandName).Distinct())
                source.Append("        registrar.Event<global::UniGame.StaticEcs.Network.NetworkCommandAcceptedEvent<").Append(command).Append(">>();\n")
                    .Append("        registrar.Event<global::UniGame.StaticEcs.Network.NetworkCommandRejectedEvent<").Append(command).Append(">>();\n");
        }

        private static void AppendRegistration(StringBuilder source, Record record, string factory, int indent = 8)
        {
            var operation = record.Kind == Kind.Component && Implements(record.Symbol, "FFS.Libraries.StaticEcs.IDisableable") ? "DisableableComponent" : record.Kind.ToString();
            source.Append(' ', indent).Append(factory).Append('.').Append(operation).Append('<').Append(record.TypeName).Append(">(new global::UniGame.StaticEcs.Network.NetworkTypeId(").Append(record.Id).Append("u)");
            if (record.Kind != Kind.Entity) source.Append(", ").Append(VersionExpression(record));
            source.Append(");\n");
        }

        private static string VersionExpression(Record record)
        {
            if (record.Kind == Kind.Component && ImplementsGeneric(record.Symbol, "FFS.Libraries.StaticEcs.IComponentConfig<T>"))
                return "global::UniGame.StaticEcs.Network.NetworkCompilerSupport.ComponentVersion<" + record.TypeName + ">()";
            if (record.Kind == Kind.Command && ImplementsGeneric(record.Symbol, "FFS.Libraries.StaticEcs.IEventConfig<T>"))
                return "global::UniGame.StaticEcs.Network.NetworkCompilerSupport.EventVersion<" + record.TypeName + ">()";
            return record.Version.ToString();
        }

        private static bool Implements(INamedTypeSymbol type, string metadataName) => type.AllInterfaces.Any(i => i.ToDisplayString() == metadataName);
        private static bool ImplementsGeneric(INamedTypeSymbol type, string metadataName) => type.AllInterfaces.Any(i => i.OriginalDefinition.ToDisplayString() == metadataName);
        private static bool HasRequiredHooks(INamedTypeSymbol type, Kind kind)
        {
            if (kind == Kind.Entity || kind == Kind.Tag || kind == Kind.Link || kind == Kind.Links || kind == Kind.Multi && type.IsUnmanagedType) return true;
            if (kind == Kind.Component) return HasComponentHooks(type);
            if (kind == Kind.Command) return HasHook(type, "Write", 0, "FFS.Libraries.StaticPack.BinaryPackWriter") && HasHook(type, "Read", 0, "FFS.Libraries.StaticPack.BinaryPackReader", "System.Byte");
            return HasHook(type, "Write", 0, "FFS.Libraries.StaticPack.BinaryPackWriter") && HasHook(type, "Read", 0, "FFS.Libraries.StaticPack.BinaryPackReader");
        }
        private static bool HasHook(INamedTypeSymbol type, string name, int arity, params string[] parameters) => type.GetMembers(name).OfType<IMethodSymbol>().Any(method =>
            !method.IsStatic && method.ReturnsVoid && method.Arity == arity && method.Parameters.Length == parameters.Length &&
            method.Parameters[0].RefKind == RefKind.Ref && Enumerable.Range(0, parameters.Length).All(i => TypeName(method.Parameters[i].Type) == parameters[i]));
        private static string TypeName(ITypeSymbol type) => type.SpecialType == SpecialType.System_Byte ? "System.Byte" : type.SpecialType == SpecialType.System_Boolean ? "System.Boolean" : type.ToDisplayString();
        private static bool HasComponentHooks(INamedTypeSymbol type)
        {
            var write = type.GetMembers("Write").OfType<IMethodSymbol>().Any(m => !m.IsStatic && m.ReturnsVoid && m.Arity == 1 && m.Parameters.Length == 2 && m.Parameters[0].RefKind == RefKind.Ref && m.Parameters[0].Type.ToDisplayString() == "FFS.Libraries.StaticPack.BinaryPackWriter");
            var read = type.GetMembers("Read").OfType<IMethodSymbol>().Any(m => !m.IsStatic && m.ReturnsVoid && m.Arity == 1 && m.Parameters.Length == 4 && m.Parameters[0].RefKind == RefKind.Ref && m.Parameters[0].Type.ToDisplayString() == "FFS.Libraries.StaticPack.BinaryPackReader" && m.Parameters[2].Type.SpecialType == SpecialType.System_Byte && m.Parameters[3].Type.SpecialType == SpecialType.System_Boolean);
            return write && read;
        }
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
            internal Record(uint id, Kind kind, string typeName, string metadataName, byte version, INamedTypeSymbol symbol, IAssemblySymbol originAssembly) { Id = id; Kind = kind; TypeName = typeName; MetadataName = metadataName; Version = version; Symbol = symbol; OriginAssembly = originAssembly; }
            internal uint Id { get; } internal Kind Kind { get; } internal string TypeName { get; } internal string MetadataName { get; } internal byte Version { get; } internal INamedTypeSymbol Symbol { get; } internal IAssemblySymbol OriginAssembly { get; }
        }
        private readonly struct Policy { internal Policy(INamedTypeSymbol type, string commandName) { Type = type; CommandName = commandName; } internal INamedTypeSymbol Type { get; } internal string CommandName { get; } }
    }
}
