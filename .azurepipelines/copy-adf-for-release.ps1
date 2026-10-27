# ==============================================================================
# Azure Data Factory Configuration Copy Script
# ==============================================================================
# Description: This script copies a development ADF configuration file and 
#              updates it for deployment to other environments (CI, UAT, PROD).
#              It replaces the ADF name and identity references throughout the file.
#
# Usage: .\copy-adf-for-release.ps1 -adfName "target-adf-name" 
#                                   -adfIdentity "target-identity-guid"
#                                   -devAdfName "source-dev-adf-name"
#                                   -devAdfIdentity "source-dev-identity-guid"
# ==============================================================================

param(
    # Target Azure Data Factory name for the new environment
    [Parameter(Mandatory=$true)]
    [string]$adfName,
    
    # Target Azure Data Factory managed identity principal ID
    [Parameter(Mandatory=$true)]
    [string]$adfIdentity,
    
    # Source development Azure Data Factory name to copy from
    [Parameter(Mandatory=$true)]
    [string]$devAdfName,
    
    # Source development Azure Data Factory managed identity principal ID
    [Parameter(Mandatory=$true)]
    [string]$devAdfIdentity
)

# ==============================================================================
# Configuration and Path Setup
# ==============================================================================

# Set the base path for the Azure Data Factory configuration files
$dataFactoryPath = ".\data-factory\factory"

# Define source and destination file paths
# Source: The development ADF configuration file to copy from
# Destination: The new ADF configuration file to create for the target environment
$sourceFile = Join-Path $dataFactoryPath "$devAdfName.json"
$destinationFile = Join-Path $dataFactoryPath "$adfName.json"

# ==============================================================================
# Main Processing Logic
# ==============================================================================

try {
    # Step 1: Validate that the source development ADF configuration file exists
    if (-not (Test-Path $sourceFile)) {
        throw "Source file '$sourceFile' does not exist."
    }

    Write-Host "Copying '$sourceFile' to '$destinationFile'..." -ForegroundColor Green
    
    # Step 2: Copy the development ADF configuration to create the target file
    # Using -Force to overwrite if the destination file already exists
    Copy-Item -Path $sourceFile -Destination $destinationFile -Force
    
    Write-Host "Reading and replacing content in '$destinationFile'..." -ForegroundColor Green
    
    # Step 3: Read the entire content of the destination file for text replacement
    # Using -Raw to read as a single string rather than an array of lines
    $content = Get-Content -Path $destinationFile -Raw
    
    # Step 4: Replace all instances of the development ADF name with the target ADF name
    # Using regex escaping to handle special characters in the names safely
    $content = $content -replace [regex]::Escape($devAdfName), $adfName
    
    # Step 5: Replace all instances of the development identity GUID with the target identity GUID
    # This updates the managed identity principal ID references in the configuration
    $content = $content -replace [regex]::Escape($devAdfIdentity), $adfIdentity
    
    # Step 6: Write the updated content back to the destination file
    # Using UTF8 encoding to ensure proper character handling
    Set-Content -Path $destinationFile -Value $content -Encoding UTF8
    
    # Step 7: Display success message and summary of changes made
    Write-Host "Successfully created '$destinationFile' with updated configuration." -ForegroundColor Green
    Write-Host "Replacements made:" -ForegroundColor Yellow
    Write-Host "  - '$devAdfName' -> '$adfName'" -ForegroundColor Yellow
    Write-Host "  - '$devAdfIdentity' -> '$adfIdentity'" -ForegroundColor Yellow
    
    # Step 8: Clean up by removing the source development ADF configuration file
    # This prevents confusion and ensures only the target environment file remains
    Write-Host "Removing source file '$sourceFile'..." -ForegroundColor Green
    Remove-Item -Path $sourceFile -Force
    Write-Host "Successfully removed '$sourceFile'." -ForegroundColor Green
}
catch {
    # Error handling: Display detailed error message and exit with error code
    # This ensures the Azure DevOps pipeline will fail if any step encounters an issue
    Write-Error "An error occurred: $($_.Exception.Message)"
    exit 1
}