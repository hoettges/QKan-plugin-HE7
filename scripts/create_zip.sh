#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Switch to plugin root
pushd "${DIR}/../qkan_he7" || (echo "Failed to switch to plugin dir" && exit 1)
  tag="$(git describe --tags "$(git rev-list --tags --max-count=1)")"
  echo "Creating qkan_he7-$tag.zip in ${DIR}"
  git archive --format zip -o "${DIR}/qkan_he7-${tag}.zip" --prefix=qkan_he7/ "${tag}"
popd