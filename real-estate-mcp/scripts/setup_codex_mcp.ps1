param(
    [string]$ServerName = "real-estate",
    [string]$ProjectRoot = "",
    [string]$DataGoKrApiKey = "",
    [string]$OdcloudApiKey = "",
    [string]$OdcloudServiceKey = "",
    [string]$OnbidApiKey = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $ProjectRoot) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Write-Host "Project root: $ProjectRoot"

$null = Get-Command codex -ErrorAction Stop
$null = Get-Command uv -ErrorAction Stop

$argsList = @("mcp", "add", $ServerName)

if ($DataGoKrApiKey) {
    $argsList += @("--env", "DATA_GO_KR_API_KEY=$DataGoKrApiKey")
}
if ($OdcloudApiKey) {
    $argsList += @("--env", "ODCLOUD_API_KEY=$OdcloudApiKey")
}
if ($OdcloudServiceKey) {
    $argsList += @("--env", "ODCLOUD_SERVICE_KEY=$OdcloudServiceKey")
}
if ($OnbidApiKey) {
    $argsList += @("--env", "ONBID_API_KEY=$OnbidApiKey")
}

$argsList += @(
    "--",
    "uv",
    "run",
    "--directory",
    $ProjectRoot,
    "python",
    "src/real_estate/mcp_server/server.py"
)

Write-Host "Registering MCP server '$ServerName'..."
& codex @argsList

if ($LASTEXITCODE -ne 0) {
    throw "codex mcp add failed with exit code $LASTEXITCODE"
}

Write-Host "Validating registration..."
& codex mcp get $ServerName --json

if ($LASTEXITCODE -ne 0) {
    throw "codex mcp get failed with exit code $LASTEXITCODE"
}

Write-Host "Done."
