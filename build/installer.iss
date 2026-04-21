; ==========================================================================
;  Duplicator Search & Destroy — Inno Setup script
; ==========================================================================
;  Expects the PyInstaller --onedir build under ..\dist\DuplicatorSearchDestroy
;  Run `build\build_installer.bat` from the repo root to produce:
;     dist\installer\DuplicatorSearchDestroy-Setup-<version>.exe
; ==========================================================================

#define MyAppName        "Duplicator Search & Destroy"
#define MyAppShortName   "DuplicatorSearchDestroy"
#define MyAppVersion     "1.0.0"
#define MyAppPublisher   "Squirrelnuttz4u"
#define MyAppURL         "https://github.com/squirrelnuttz4u/duplicator-search-destroy"
#define MyAppExeName     "DuplicatorSearchDestroy.exe"
; Stable GUID — never change this between releases or upgrades will break.
#define MyAppId          "{{B2D14E6C-3E12-4A6F-9D7D-DF3AE1C6A1F0}"

[Setup]
AppId={#MyAppId}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}/issues
AppUpdatesURL={#MyAppURL}/releases
VersionInfoVersion={#MyAppVersion}
VersionInfoProductName={#MyAppName}
VersionInfoCompany={#MyAppPublisher}

; Offer per-user OR per-machine install — the user picks on the first page.
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog

; {autopf} = Program Files (per-machine) or %LOCALAPPDATA%\Programs (per-user)
DefaultDirName={autopf}\{#MyAppShortName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
UninstallDisplayIcon={app}\{#MyAppExeName}
UninstallDisplayName={#MyAppName}

; Disk / output
OutputDir=..\dist\installer
OutputBaseFilename={#MyAppShortName}-Setup-{#MyAppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
LZMAUseSeparateProcess=yes

; Windows 10 1809 minimum (smbprotocol + PySide6 6.7 both require this)
MinVersion=10.0.17763
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible

; Cosmetic
WizardStyle=modern
DisableDirPage=auto
DisableProgramGroupPage=auto
ShowLanguageDialog=no
CloseApplications=force
RestartApplications=no

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon";    Description: "{cm:CreateDesktopIcon}";  GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1

[Files]
; Ship the entire PyInstaller --onedir output. recursesubdirs walks _internal\
; which contains PySide6, smbprotocol, cryptography DLLs, etc.
Source: "..\dist\DuplicatorSearchDestroy\*"; DestDir: "{app}"; \
    Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autoprograms}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; \
    Flags: nowait postinstall skipifsilent

[UninstallDelete]
; The SQLite inventory and logs live under %APPDATA% by default — we do NOT
; touch that directory on uninstall so reinstalling doesn't destroy months
; of indexing work. If the operator really wants a clean slate, they can
; delete %APPDATA%\DuplicatorSearchDestroy\ manually.
Type: filesandordirs; Name: "{app}"

[Code]
{
  Prevent downgrades and force-close any running instance before overwriting.
}
function InitializeSetup(): Boolean;
var
  RunningPath: String;
begin
  Result := True;
  RunningPath := ExpandConstant('{app}\{#MyAppExeName}');
  { Inno's built-in CloseApplications directive above handles the rest. }
end;
