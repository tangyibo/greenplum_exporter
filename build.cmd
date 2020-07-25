::	
:: Author: tang	
:: Date: 2020-03-06
::	
@echo off
title greenplum_exporter
setlocal enabledelayedexpansion
cls

set binFolder=bin\

if not exist %binFolder% (				
	md %binFolder%
)

go clean
go mod download
go build -o %binFolder%\greenplum_exporter.exe

echo "build over!"
:exit
pause
