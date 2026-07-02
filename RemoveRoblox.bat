@echo off
echo Закрываем Roblox...

taskkill F IM RobloxPlayerBeta.exe nul 2&1
taskkill F IM RobloxPlayerInstaller.exe nul 2&1
taskkill F IM RobloxStudioBeta.exe nul 2&1

echo Удаляем папки Roblox...

rmdir S Q %LOCALAPPDATA%Roblox
rmdir S Q %LOCALAPPDATA%TempRoblox
rmdir S Q %USERPROFILE%AppDataLocalLowRoblox

echo Удаляем ярлыки...

del F Q %USERPROFILE%DesktopRoblox Player.lnk nul 2&1
del F Q %APPDATA%MicrosoftWindowsStart MenuProgramsRoblox.lnk nul 2&1

echo Удаляем записи реестра Roblox...

reg delete HKCUSoftwareRoblox f nul 2&1
reg delete HKLMSOFTWARERoblox f nul 2&1
reg delete HKLMSOFTWAREWOW6432NodeRoblox f nul 2&1

echo Очистка завершена.
pause