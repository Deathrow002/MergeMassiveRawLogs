# ParseLogs.ps1
# PowerShell script to parse raw logs and convert to structured JSON format
# Separates URL, Headers, and Body for each log entry
#
# Usage:
#   .\ParseLogs.ps1 -InputPath ".\data\input" -OutputPath ".\data\output\parsed.json"
#   .\ParseLogs.ps1 -InputFile ".\data\input\log1.txt" -OutputPath ".\output.json"

param(
    [string]$InputPath = ".\data\input",
    [string]$InputFile = "",
    [string]$OutputPath = ".\data\output\parsed.json",
    [int]$MaxThreads = [Environment]::ProcessorCount
)

function Parse-LogEntry {
    param([string]$RawEntry)
    
    $Entry = @{
        Timestamp = $null
        ThreadName = $null
        Level = $null
        TagId = $null
        ClassName = $null
        Action = $null
        CamelBreadcrumbId = $null
        ResponseCode = $null
        Url = $null
        ExecutionTimeMs = $null
        ConnectTimeout = $null
        ReadTimeout = $null
        RequestMethod = $null
        Headers = @{}
        Body = $null
    }
    
    $Lines = $RawEntry -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" -and $_ -ne "--------------------------------------" }
    
    if ($Lines.Count -eq 0) { return $null }
    
    # Parse header line: [thread][level][timestamp][tagId:xxx][class] [ACTION][camelId]
    $HeaderLine = $Lines[0]
    
    # Extract thread name
    if ($HeaderLine -match '^\[([^\]]+)\]') {
        $Entry.ThreadName = $Matches[1]
    }
    
    # Extract level
    if ($HeaderLine -match '\]\[(INFO|DEBUG|WARN|ERROR|TRACE)\]\[') {
        $Entry.Level = $Matches[1]
    }
    
    # Extract timestamp
    if ($HeaderLine -match '\[(\d{4}-\d{2}-\d{2}T[\d:\.]+[+-]\d{4})\]') {
        $Entry.Timestamp = $Matches[1]
    }
    
    # Extract tagId
    if ($HeaderLine -match '\[tagId:([^\]]+)\]') {
        $Entry.TagId = $Matches[1]
    }
    
    # Extract class name (e.g., com.sde.modelsuite.proxy.BaseProxy)
    if ($HeaderLine -match '\[(com\.[a-zA-Z\._]+)\]') {
        $Entry.ClassName = $Matches[1]
    }
    
    # Extract action (SEND/RECEIVE)
    if ($HeaderLine -match '\[(SEND|RECEIVE)\]') {
        $Entry.Action = $Matches[1]
    }
    
    # Extract camelBreadcrumbId
    if ($HeaderLine -match '\[camelbreadcrumbId:([^\]]+)\]') {
        $Entry.CamelBreadcrumbId = $Matches[1]
    }
    
    # Parse remaining lines
    $JsonStartIndex = -1
    $HeadersSection = $false
    
    for ($i = 1; $i -lt $Lines.Count; $i++) {
        $Line = $Lines[$i]
        
        # Check if this is the start of JSON body
        if ($Line -match '^\{') {
            $JsonStartIndex = $i
            break
        }
        
        # Parse Response Code (RECEIVE)
        if ($Line -match 'Response Code = \[(\d+)\]') {
            $Entry.ResponseCode = [int]$Matches[1]
        }
        
        # Parse Receive URL and execution time
        if ($Line -match 'Receive URL = \[([^\]]+)\], execution time = \[(\d+)\]') {
            $Entry.Url = $Matches[1]
            $Entry.ExecutionTimeMs = [int]$Matches[2]
        }
        
        # Parse Send URL with parameters
        if ($Line -match 'Send URL = \[([^\]]+)\]') {
            $Entry.Url = $Matches[1]
            
            if ($Line -match 'connectTimeout = \[(\d+)\]') {
                $Entry.ConnectTimeout = [int]$Matches[1]
            }
            if ($Line -match 'readTimeout = \[(\d+)\]') {
                $Entry.ReadTimeout = [int]$Matches[1]
            }
            if ($Line -match 'requestMethod = \[([^\]]+)\]') {
                $Entry.RequestMethod = $Matches[1]
            }
        }
        
        # Parse headers (Key : Value format)
        if ($Line -match '^([A-Za-z\-]+)\s*:\s*(.+)$') {
            $HeaderName = $Matches[1].Trim()
            $HeaderValue = $Matches[2].Trim()
            $Entry.Headers[$HeaderName] = $HeaderValue
        }
    }
    
    # Extract JSON body
    if ($JsonStartIndex -ge 0) {
        $JsonLines = $Lines[$JsonStartIndex..($Lines.Count - 1)] -join "`n"
        try {
            $Entry.Body = $JsonLines | ConvertFrom-Json
        }
        catch {
            $Entry.Body = $JsonLines  # Keep as raw string if not valid JSON
        }
    }
    
    # Clean up null/empty values
    $CleanEntry = @{}
    foreach ($Key in $Entry.Keys) {
        $Value = $Entry[$Key]
        if ($null -ne $Value) {
            if ($Value -is [hashtable] -and $Value.Count -eq 0) { continue }
            if ($Value -is [string] -and [string]::IsNullOrEmpty($Value)) { continue }
            $CleanEntry[$Key] = $Value
        }
    }
    
    return $CleanEntry
}

function Parse-LogFile {
    param([string]$FilePath)
    
    $Content = [System.IO.File]::ReadAllText($FilePath, [System.Text.Encoding]::UTF8)
    
    # Split by separator line
    $RawEntries = $Content -split '(?=--------------------------------------\r?\n\[)'
    
    $ParsedEntries = @()
    
    foreach ($RawEntry in $RawEntries) {
        if ([string]::IsNullOrWhiteSpace($RawEntry)) { continue }
        
        $Parsed = Parse-LogEntry -RawEntry $RawEntry
        if ($null -ne $Parsed -and $Parsed.Count -gt 0) {
            $ParsedEntries += $Parsed
        }
    }
    
    return $ParsedEntries
}

# Main execution
Write-Host "Log Parser - Converting raw logs to structured JSON"
Write-Host "=================================================="

$AllEntries = @()

if ($InputFile -ne "" -and (Test-Path $InputFile -PathType Leaf)) {
    # Single file mode
    Write-Host "Parsing file: $InputFile"
    $AllEntries = Parse-LogFile -FilePath $InputFile
}
elseif (Test-Path $InputPath -PathType Container) {
    # Directory mode
    $Files = Get-ChildItem -Path $InputPath -Filter "*.txt" -File | Sort-Object Name
    $TotalFiles = $Files.Count
    
    if ($TotalFiles -eq 0) {
        Write-Host "No .txt files found in input directory."
        exit 0
    }
    
    Write-Host "Found $TotalFiles files to parse"
    
    $FileIndex = 0
    foreach ($File in $Files) {
        $FileIndex++
        Write-Host "[$FileIndex/$TotalFiles] Parsing: $($File.Name)"
        
        $Entries = Parse-LogFile -FilePath $File.FullName
        $AllEntries += $Entries
        
        Write-Host "  Found $($Entries.Count) log entries"
    }
}
else {
    Write-Error "Input path does not exist: $InputPath"
    exit 1
}

# Create output directory if needed
$OutputDir = Split-Path $OutputPath -Parent
if ($OutputDir -and -not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

# Write output JSON
Write-Host ""
Write-Host "Writing $($AllEntries.Count) entries to: $OutputPath"

$JsonOutput = $AllEntries | ConvertTo-Json -Depth 10
[System.IO.File]::WriteAllText($OutputPath, $JsonOutput, [System.Text.Encoding]::UTF8)

Write-Host "Parse completed successfully!"
