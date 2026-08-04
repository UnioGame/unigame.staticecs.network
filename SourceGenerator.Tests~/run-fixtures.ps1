$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

function Invoke-BoundedBuild([string]$project, [int]$timeoutSeconds = 30) {
    $stdout = New-TemporaryFile
    $stderr = New-TemporaryFile
    try {
        $process = Start-Process dotnet -ArgumentList @('build', $project, '--nologo') -WorkingDirectory $root -NoNewWindow -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        if (-not $process.WaitForExit($timeoutSeconds * 1000)) {
            $process.Kill($true)
            throw "Timed out building $project"
        }
        return [pscustomobject]@{ ExitCode = $process.ExitCode; Output = (Get-Content $stdout -Raw) + (Get-Content $stderr -Raw) }
    }
    finally {
        Remove-Item -LiteralPath $stdout, $stderr -Force
    }
}

function Assert-Pass([string]$project) {
    $result = Invoke-BoundedBuild $project
    if ($result.ExitCode -ne 0) { throw "Expected PASS for $project`n$($result.Output)" }
}

function Assert-Diagnostic([string]$project, [string[]]$ids) {
    $result = Invoke-BoundedBuild $project
    if ($result.ExitCode -eq 0) { throw "Expected compiler failure for $project" }
    foreach ($id in $ids) {
        if ($result.Output -notmatch [regex]::Escape($id)) { throw "Missing $id from $project`n$($result.Output)" }
    }
}

function Invoke-BoundedRun([string]$define = '') {
    $stdout = New-TemporaryFile
    $stderr = New-TemporaryFile
    try {
        $arguments = @('run', '--project', 'SourceGenerator.Tests~/SourceGenerator.Tests.csproj', '--no-restore')
        if ($define) { $arguments += "-p:DefineConstants=$define" }
        $process = Start-Process dotnet -ArgumentList $arguments -WorkingDirectory $root -NoNewWindow -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        if (-not $process.WaitForExit(30000)) { $process.Kill($true); throw 'Timed out executing generated endpoint fixture' }
        $output = (Get-Content $stdout -Raw) + (Get-Content $stderr -Raw)
        if ($process.ExitCode -ne 0) { throw "Generated endpoint execution failed`n$output" }
        if ($output -notmatch 'SCHEMA:(.+)') { throw "Generated endpoint fingerprint missing`n$output" }
        return $Matches[1].Trim()
    }
    finally { Remove-Item -LiteralPath $stdout, $stderr -Force }
}

Assert-Pass 'SourceGenerator.Shared.Tests~/SourceGenerator.Shared.Tests.csproj'
Assert-Pass 'SourceGenerator.Empty.Tests~/SourceGenerator.Empty.Tests.csproj'
$emptyGenerated = 'SourceGenerator.Empty.Tests~/obj/generated/StaticEcs.Network.Generator/UniGame.StaticEcs.Network.Generator.NetworkSourceGenerator/NetworkManifest.g.cs'
if (Test-Path $emptyGenerated) { throw 'Generator emitted a manifest for an assembly without network types or endpoints' }
Assert-Pass 'SourceGenerator.Tests~/SourceGenerator.Tests.csproj'
$generated = Get-Content 'SourceGenerator.Tests~/obj/generated/StaticEcs.Network.Generator/UniGame.StaticEcs.Network.Generator.NetworkSourceGenerator/GeneratedServerNetwork.g.cs' -Raw
foreach ($required in @('ComponentVersion<global::Shared.Position>()', 'EventVersion<global::Shared.Move>()', 'factory.Policy<global::Shared.Move, global::Demo.MovePolicy>()')) {
    if (-not $generated.Contains($required)) { throw "Missing generated AOT/version assertion: $required" }
}
$declaredVersionFingerprint = Invoke-BoundedRun
$changedVersionFingerprint = Invoke-BoundedRun 'NETWORK_VERSION_MISMATCH'
if ($declaredVersionFingerprint -eq $changedVersionFingerprint) { throw 'Generated endpoint fingerprint ignored the changed Static ECS config version' }

Assert-Diagnostic 'SourceGenerator.MissingPolicy.Tests~/SourceGenerator.MissingPolicy.Tests.csproj' @('NETV2009')
Assert-Diagnostic 'SourceGenerator.DuplicatePolicy.Tests~/SourceGenerator.DuplicatePolicy.Tests.csproj' @('NETV2010')
Assert-Diagnostic 'SourceGenerator.MissingHooks.Tests~/SourceGenerator.MissingHooks.Tests.csproj' @('NETV2007')
Assert-Diagnostic 'SourceGenerator.SharedOnly.Tests~/SourceGenerator.SharedOnly.Tests.csproj' @('NETV2006')
Assert-Pass 'SourceGenerator.BadManifest.Tests~/SourceGenerator.BadManifest.Tests.csproj'
Assert-Diagnostic 'SourceGenerator.InvalidManifest.Tests~/SourceGenerator.InvalidManifest.Tests.csproj' @('NETV2002', 'NETV2008')

Write-Host 'PASS: generated endpoint execution equality/version mismatch and exact diagnostics NETV2002, NETV2006-NETV2010.'
