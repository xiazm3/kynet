param([switch]$SkipJavaBuild)

$ErrorActionPreference = 'Stop'
$clientRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$repoRoot = (Resolve-Path (Join-Path $clientRoot '../..')).Path
$artifacts = Join-Path $clientRoot 'artifacts'
New-Item -ItemType Directory -Force -Path $artifacts | Out-Null

Push-Location $repoRoot
try {
    if (-not $SkipJavaBuild) {
        & mvn -pl examples/example-login-service,kynet-registry -am install -DskipTests
        if ($LASTEXITCODE -ne 0) { throw 'Java build failed.' }
    }
    $classpathFile = Join-Path $artifacts 'java-classpath.txt'
    & mvn -pl examples/example-login-service dependency:build-classpath "-Dmdep.outputFile=$classpathFile" -DincludeScope=test
    if ($LASTEXITCODE -ne 0) { throw 'Could not resolve the Java classpath.' }
} finally { Pop-Location }

Push-Location $clientRoot
try {
    & dotnet restore Kynet.Client.sln --configfile NuGet.Config
    if ($LASTEXITCODE -ne 0) { throw 'NuGet restore failed.' }
    & dotnet build Kynet.Client.sln -c Release --no-restore
    if ($LASTEXITCODE -ne 0) { throw 'Client build failed.' }

    $portReservations = @()
    try {
        # Reserve together so the operating system cannot return the same port twice.
        1..4 | ForEach-Object {
            $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
            $listener.Start()
            $portReservations += $listener
        }
        $ports = @($portReservations | ForEach-Object { $_.LocalEndpoint.Port })
    } finally { $portReservations | ForEach-Object { $_.Stop() } }

    $runId = [Guid]::NewGuid().ToString('N')
    $readyFile = Join-Path $artifacts "$runId.ready"
    $stopFile = Join-Path $artifacts "$runId.stop"
    $classDirs = @('kynet-common', 'kynet-framework', 'kynet-registry', 'examples/example-login-service') |
        ForEach-Object { Join-Path $repoRoot "$_/target/classes" }
    $classpath = ($classDirs -join [IO.Path]::PathSeparator) + [IO.Path]::PathSeparator + (Get-Content $classpathFile -Raw).Trim()
    $start = [Diagnostics.ProcessStartInfo]::new((Get-Command java).Source)
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.WorkingDirectory = $repoRoot
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    @('--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED',
        '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-cp', $classpath,
        (Join-Path $PSScriptRoot 'GatewayInteropServer.java')) + @($ports | ForEach-Object { "$_" }) + @($readyFile, $stopFile) |
        ForEach-Object { $start.ArgumentList.Add($_) }
    $process = [Diagnostics.Process]::Start($start)
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    try {
        $startup = [Diagnostics.Stopwatch]::StartNew()
        while (-not (Test-Path -LiteralPath $readyFile)) {
            if ($process.HasExited) { throw "Java gateway exited: $($stderr.GetAwaiter().GetResult())" }
            if ($startup.Elapsed.TotalSeconds -gt 45) { throw 'Java gateway startup timed out.' }
            Start-Sleep -Milliseconds 100
        }
        $endpoints = @("tcp://127.0.0.1:$($ports[1])", "udp://127.0.0.1:$($ports[2])", "ws://127.0.0.1:$($ports[3])/ws")
        foreach ($endpoint in $endpoints) {
            & dotnet run --project samples/Kynet.Client.Console -c Release --no-build -- $endpoint --verify
            if ($LASTEXITCODE -ne 0) { throw "Client interoperability failed for $endpoint" }
        }
    } finally {
        [IO.File]::WriteAllText($stopFile, 'stop')
        if (-not $process.WaitForExit(15000)) { $process.Kill($true); $process.WaitForExit() }
        [IO.File]::WriteAllText((Join-Path $artifacts 'java-stdout.log'), $stdout.GetAwaiter().GetResult())
        [IO.File]::WriteAllText((Join-Path $artifacts 'java-stderr.log'), $stderr.GetAwaiter().GetResult())
        $process.Dispose()
        Remove-Item -LiteralPath $readyFile, $stopFile -ErrorAction SilentlyContinue
    }
} finally { Pop-Location }
