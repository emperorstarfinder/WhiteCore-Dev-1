#!/bin/bash
# Run prebuild to configure and create the appropriate Solution and Project files for building WhiteCore-Sim
#
# June 2021
# Rowan Deppeler <greythane@gmail.com>
#
# Updated Dec 2018 to use NET 4.6 framework and msbuild (included in Mono V5+)
# Updated June 2012 to provide xbuild option

ARCH="x64"
CONFIG="Debug"
BUILD=true
VERSIONONLY=false

USAGE="[-c <config>] -a <arch> -v"
LONG_USAGE="Configuration options to pass to prebuild environment

Options:
  -c|--config Build configuration Debug (default) or Release
  -a|--arch Architecture to target x86 or x64 (default)
  -b|--build Build after configuration No (default) or Yes
  -v|--version Update version details only
"
# get the current system architecture
if (( 1 == 1<<32 )); then
    ARCH="x86";
    echo "x86 architecture detected";
else
    echo "x64 architecture found";
fi

# check if prompting needed
if [ $# -gt 0 ]; then
    read -p "Architecture to use? (x86, x64) [$ARCH]: " bits
    if [[ $bits == "x86" ]]; then ARCH="x86"; fi
    if [[ $bits == "86" ]]; then ARCH="x86"; fi
    if [[ $bits == "x64" ]]; then ARCH="x64"; fi
    if [[ $bits == "64" ]]; then ARCH="x64"; fi

    read -p "Configuration? (Release, Debug) [$CONFIG]: " conf
    if [[ $conf == "Release" ]]; then CONFIG="Release"; fi
    if [[ $conf == "release" ]]; then CONFIG="Release"; fi

	bld="No"
	if [[ $BUILD == true ]]; then bld="Yes"; fi

    read -p "Build immediately? (Yes, No) [$bld]: " bld
    if [[ $bld == "Yes" ]]; then BUILD=true; fi
    if [[ $bld == "yes" ]]; then BUILD=true; fi
    if [[ $bld == "y" ]]; then BUILD=true; fi

else

# command line params supplied
while case "$#" in 0) break ;; esac
do
  case "$1" in
    -c|--config)
      shift
      CONFIG="$1"
      ;;
    -a|--arch)
      shift
      ARCH="$1"
      ;;
    -b|--build)
      BUILD=true
      shift
      ;;
    -v|--version)
      VERSIONONLY=true
      ;;
    -h|--help)
      echo "$USAGE"
      echo "$LONG_USAGE"
      exit
      ;;
    *)
      echo "Illegal option!"
      echo "$USAGE"
      echo "$LONG_USAGE"
      exit
      ;;
  esac
  shift
done

fi

# Configuring WhiteCore-Sim
if ! ${VERSIONONLY:=true}; then
  echo "Configuring WhiteCore-Sim $ARCH $CONFIG build"
  mono ./Prebuild.exe /target vs2015 /targetframework v4_6 /conditionals "LINUX"
fi

# Update version info
if [ -d ".git" ]; then
  git log --pretty=format:"WhiteCore 0.9.6 Dev (%cd.%h)" --date=short -n 1 > WhiteCoreSim/bin/.version;
  echo "Version info updated"
fi

# Build WhiteCore-Sim
if $BUILD ; then
  echo Building WhiteCore-Sim
  msbuild WhiteCore.sln /property:Configuration="$CONFIG" /property:Platform="$ARCH"
  # xbuild WhiteCore.sln /property:Configuration="$CONFIG" /property:Platform="$ARCH"
  echo Finished Building WhiteCore
  echo Thank you for choosing WhiteCore-Sim
  echo Please report any errors to our Github Issue Tracker https://github.com/WhiteCoreSim/WhiteCore-Dev/issues
else
  echo "WhiteCore-Sim has been configured to compile with $ARCH $CONFIG options"
  echo "To manually build, enter 'msbuild WhiteCore.sln' at the command prompt"

fi
