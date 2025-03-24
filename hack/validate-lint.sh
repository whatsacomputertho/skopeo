#!/bin/bash

errors=$($GOBIN/golangci-lint run --build-tags "${BUILDTAGS}" 2>&1)

if [ "$?" -eq 0 ]; then
	echo 'Congratulations!  All Go source files have been linted.'
else
	{
		echo "Errors from golangci-lint:"
		echo "$errors"
		echo
		echo 'Please fix the above errors. You can test via "golangci-lint" and commit the result.'
		echo
	} >&2
	exit 1
fi
