﻿<#
.SYNOPSIS
Compiles all known resources.qrc to resources.py using QGIS's pyrcc4.
#>

$path = ""
$PYRCC = "pyrcc4.exe"

function Pause-Exit($code)
{
    Write-Host 'Press Enter to exit.'
    [void][System.Console]::ReadKey($true)
    exit $code
}
if ($inst = Get-ItemProperty -Path "HKLM:\SOFTWARE\QGIS 2.18" -Name InstallPath -ErrorAction SilentlyContinue)
{
    $path = $inst.InstallPath
}
else
{
    $path = Read-Host -Prompt "Could not find installation path of QGIS 2.18.`nPlease enter it manually"
}

if ([string]::IsNullOrEmpty($path))
{
    Write-Error -Message "Empty path entered or found. Aborting."
    Pause-Exit(1)
}

Write-Host "Using '$path'"

$PYRCC = $path + "\bin\" + $PYRCC

if (-not (Test-Path $PYRCC))
{
    Write-Error -Message "pyrcc4 seems to be missing in your installation. Aborting."
    Pause-Exit(1)
}

# Init Q4W environment, replicates o4w_env.bat
[Environment]::SetEnvironmentVariable("PATH", [string]::Join(";", @($path, "%WINDIR%\system32", "%WINDIR%", "%WINDIR%\system32\WBem")))
[Environment]::SetEnvironmentVariable("PYTHONPATH", $inst.InstallPath + "\apps\qgis-ltr\python")
[Environment]::SetEnvironmentVariable("PYTHONHOME", $inst.InstallPath + "\apps\Python27")
[Environment]::SetEnvironmentVariable("QT_PLUGIN_PATH", $inst.InstallPath + "\apps\qgis-ltr\qtplugins;" + $inst.InstallPath + "\apps\qt4\plugins")
[Environment]::SetEnvironmentVariable("O4W_QT_LIBRARIES", $inst.InstallPath + "\lib\")
[Environment]::SetEnvironmentVariable("O4W_QT_HEADERS", $inst.InstallPath + "\include\qt4")
[Environment]::SetEnvironmentVariable("O4W_QT_PREFIX", $inst.InstallPath + "\apps\qt4")
[Environment]::SetEnvironmentVariable("O4W_QT_PLUGINS", $inst.InstallPath + "\apps\qt4\plugins")
[Environment]::SetEnvironmentVariable("OSGEO4W_ROOT", $inst.InstallPath)

$RESOURCE_FILES=@(
	"../qkan_he7/exporthe/resources.qrc"
	"../qkan_he7/ganglinienhe/resources.qrc"
	"../qkan_he7/importhe/resources.qrc"
)

foreach ($element in $RESOURCE_FILES)
{
    $target_file = $element.Replace(".qrc", ".py")
    Write-Host "Building $element"
    & $PYRCC -o $target_file $element
}
Pause-Exit(0)