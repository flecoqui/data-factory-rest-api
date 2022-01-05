#!/bin/bash
# shellcheck source-path=SCRIPTDIR/../scripts
#
# executable
#
# install tools in the development environment

set -e
export terraformVersion="1.0.2"

function installTerraform() {
    pushd /tmp > /dev/null
    envArchitecture=$(arch)

    if [ "${envArchitecture}" = "aarch64" ]; then
        envArchitecture="arm64"
    fi

    if [ "${envArchitecture}" = "x86_64" ]; then
        envArchitecture="amd64"
    fi

    # Install Terraform
    terraformReleaseUrl="https://releases.hashicorp.com/terraform"
    curl -LO "${terraformReleaseUrl}/${terraformVersion}/terraform_${terraformVersion}_linux_${envArchitecture}.zip"
    unzip -o "terraform_${terraformVersion}_linux_${envArchitecture}.zip" -d "/usr/local/bin"
    chmod +x /usr/local/bin/terraform
    rm "terraform_${terraformVersion}_linux_${envArchitecture}.zip"

    popd > /dev/null
}
function installAzureCli() {
    # Installing az-cli via "pip" install Python dependencies in conflicts with development around security & login
    # So instead, we will use the deb package to do so (not from Universe repo, as outdated). The script below adds a repo
    # and install the latest version of the cli
    # Note: the deb package does not support arm64 architecture, hence the test first

    pushd /tmp > /dev/null
    envArchitecture=$(arch)

    if [ "${envArchitecture}" = "x86_64" ]; then
        sudo curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
    fi

    popd > /dev/null
}

installAzureCli
installTerraform