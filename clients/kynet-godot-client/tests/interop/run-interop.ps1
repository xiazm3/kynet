param(
    [string]$GodotPath = 'godot',
    [switch]$SkipJavaBuild,
    [string]$ClasspathFile
)

$ErrorActionPreference = 'Stop'
if ($PSVersionTable.PSVersion.Major -lt 7) { throw 'PowerShell 7 or newer is required.' }
$godotExecutable = (Get-Command $GodotPath -ErrorAction Stop).Source
$clientRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$repoRoot = (Resolve-Path (Join-Path $clientRoot '../..')).Path
$artifacts = Join-Path $clientRoot 'artifacts'
New-Item -ItemType Directory -Force -Path $artifacts | Out-Null
if ($ClasspathFile) { $ClasspathFile = (Resolve-Path -LiteralPath $ClasspathFile).Path }

Push-Location $repoRoot
try {
    if (-not $SkipJavaBuild) {
        & mvn -pl examples/example-login-service,kynet-registry -am install -DskipTests
        if ($LASTEXITCODE -ne 0) { throw 'Java build failed.' }
    }
    if (-not $ClasspathFile) {
        $ClasspathFile = Join-Path $artifacts 'java-classpath.txt'
        & mvn -pl examples/example-login-service dependency:build-classpath "-Dmdep.outputFile=$ClasspathFile" -DincludeScope=test
        if ($LASTEXITCODE -ne 0) { throw 'Could not resolve Java classpath.' }
    }
    $ClasspathFile = (Resolve-Path -LiteralPath $ClasspathFile).Path
} finally { Pop-Location }

$reservations = @()
try {
    1..4 | ForEach-Object {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
        $listener.Start()
        $reservations += $listener
    }
    $ports = @($reservations | ForEach-Object { $_.LocalEndpoint.Port })
} finally { $reservations | ForEach-Object { $_.Stop() } }

$runId = [Guid]::NewGuid().ToString('N')
$readyFile = Join-Path $artifacts "$runId.ready"
$stopFile = Join-Path $artifacts "$runId.stop"
$classDirs = @('kynet-common', 'kynet-framework', 'kynet-registry', 'examples/example-login-service') |
    ForEach-Object { Join-Path $repoRoot "$_/target/classes" }
$classpath = ($classDirs -join [IO.Path]::PathSeparator) + [IO.Path]::PathSeparator + (Get-Content -LiteralPath $ClasspathFile -Raw).Trim()
$start = [Diagnostics.ProcessStartInfo]::new((Get-Command java).Source)
$start.UseShellExecute = $false
$start.CreateNoWindow = $true
$start.WorkingDirectory = $repoRoot
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
# Reuse the C# client's real registry/LoginAction/Netty integration fixture.
$fixture = Join-Path $repoRoot 'clients/kynet-client-mono/tests/interop/GatewayInteropServer.java'
@('--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED',
    '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-cp', $classpath, $fixture) +
    @($ports | ForEach-Object { "$_" }) + @($readyFile, $stopFile) |
    ForEach-Object { $start.ArgumentList.Add($_) }
$process = [Diagnostics.Process]::Start($start)
$stdout = $process.StandardOutput.ReadToEndAsync()
$stderr = $process.StandardError.ReadToEndAsync()
$previousAppData = $env:APPDATA
$previousLocalAppData = $env:LOCALAPPDATA
try {
    $startup = [Diagnostics.Stopwatch]::StartNew()
    while (-not (Test-Path -LiteralPath $readyFile)) {
        if ($process.HasExited) { throw "Java gateway exited: $($stderr.GetAwaiter().GetResult())" }
        if ($startup.Elapsed.TotalSeconds -gt 45) { throw 'Java gateway startup timed out.' }
        Start-Sleep -Milliseconds 100
    }
    $env:APPDATA = Join-Path $artifacts 'appdata'
    $env:LOCALAPPDATA = Join-Path $artifacts 'localappdata'
    $endpoints = @("tcp://127.0.0.1:$($ports[1])", "udp://127.0.0.1:$($ports[2])", "ws://127.0.0.1:$($ports[3])/ws")
    foreach ($endpoint in $endpoints) {
        & $godotExecutable --headless --path $clientRoot --script res://tests/interop/verify_gateway.gd -- $endpoint
        if ($LASTEXITCODE -ne 0) { throw "Godot interoperability failed for $endpoint" }
    }
} finally {
    $env:APPDATA = $previousAppData
    $env:LOCALAPPDATA = $previousLocalAppData
    [IO.File]::WriteAllText($stopFile, 'stop')
    if (-not $process.WaitForExit(15000)) { $process.Kill($true); $process.WaitForExit() }
    [IO.File]::WriteAllText((Join-Path $artifacts 'java-stdout.log'), $stdout.GetAwaiter().GetResult())
    [IO.File]::WriteAllText((Join-Path $artifacts 'java-stderr.log'), $stderr.GetAwaiter().GetResult())
    $process.Dispose()
    Remove-Item -LiteralPath $readyFile, $stopFile -ErrorAction SilentlyContinue
}
