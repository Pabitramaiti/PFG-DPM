#!/bin/bash
source version.properties

# su -
# whoami
# yum install zip

GIT_COMMIT=$(git rev-parse --short HEAD)
ARTIFACT_NAME="ics-dpm-xsd-di${BUILD_NUMBER}_${cm_version}-${GIT_COMMIT}"
 
if [[ -z "${ARTIFACT_NAME}" ]]; then
    echo "ERROR: ARTIFACT_NAME is required to be set as an environment variable. Example:
	
		export ARTIFACT_NAME=Project-CM_1.0.0 ; ./build.sh
	
	"
    exit 11
fi

if [[ -z "${WORKSPACE}" ]]; then
    echo "ERROR: WORKSPACE is expected to be set by Jenkins
	
		To run this locally, you need to set the variable to the project root:
	
		export WORKSPACE=$(pwd) ; ./build.sh
	
	"
    exit 11
fi

echo "${ARTIFACT_NAME}"
echo "test"
export CM_ARTIFACT_NAME=${ARTIFACT_NAME}.tar


mkdir $ARTIFACT_NAME; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem creating the artifact directory"
	exit 3
fi

# cp *.xsd $ARTIFACT_NAME
cp metadata/xsd/*.xsd $ARTIFACT_NAME; RC=$?

if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem creating artifact content"
	exit 4
fi

echo "${ARTIFACT_NAME}"

tar -cvf ${CM_ARTIFACT_NAME} -C $ARTIFACT_NAME/ .; RC=$?
if [[ $RC != 0 ]]; then
	echo "FAILURE! There was a problem creating the artifact archive"
	exit 5
fi

# find . ! -name ${CM_ARTIFACT_NAME} -delete

echo "Successfully created: ${CM_ARTIFACT_NAME}"
